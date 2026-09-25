import asyncio
import json
import logging
import os
import ssl
import uuid
from typing import Any, Dict, Literal, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Load Environment Variables & cTrader Open API Credentials
# ---------------------------------------------------------------------------
load_dotenv()

CTRADER_CLIENT_ID: str = os.environ.get("CTRADER_CLIENT_ID", "")
CTRADER_CLIENT_SECRET: str = os.environ.get("CTRADER_CLIENT_SECRET", "")
CTRADER_ACCOUNT_ID: str = os.environ.get("CTRADER_ACCOUNT_ID", "")
CTRADER_ACCESS_TOKEN: str = os.environ.get("CTRADER_ACCESS_TOKEN", "")
CTRADER_ENVIRONMENT: str = os.environ.get("CTRADER_ENVIRONMENT", "demo").strip().lower()

# cTrader Open API V2 WebSocket Endpoints (Port 5036 for JSON Protocol)
CTRADER_WS_URL = (
    "wss://demo.ctraderapi.com:5036"
    if CTRADER_ENVIRONMENT == "demo"
    else "wss://live.ctraderapi.com:5036"
)

# In-memory symbol cache (Symbol Name -> Symbol ID) to optimize latency
SYMBOL_CACHE: Dict[str, int] = {}

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("TradingView-cTrader")

# ---------------------------------------------------------------------------
# cTrader Open API Protocol Constants (Open API V2 JSON Payload Types)
# ---------------------------------------------------------------------------
class ProtoOAPayloadType:
    PROTO_OA_APPLICATION_AUTH_REQ = 2100
    PROTO_OA_APPLICATION_AUTH_RES = 2101
    PROTO_OA_ACCOUNT_AUTH_REQ = 2102
    PROTO_OA_ACCOUNT_AUTH_RES = 2103
    PROTO_OA_SYMBOLS_LIST_REQ = 2114
    PROTO_OA_SYMBOLS_LIST_RES = 2115
    PROTO_OA_NEW_ORDER_REQ = 2106
    PROTO_OA_EXECUTION_EVENT = 2126
    PROTO_OA_ORDER_ERROR_EVENT = 2132
    PROTO_OA_ERROR_RES = 2142


class ProtoOATradeSide:
    BUY = 1
    SELL = 2


class ProtoOAOrderType:
    MARKET = 1
    LIMIT = 2
    STOP = 3


# ---------------------------------------------------------------------------
# FastAPI Application & Pydantic Models
# ---------------------------------------------------------------------------
app = FastAPI(
    title="TradingView to cTrader Webhook Server",
    description="Webhook server for receiving TradingView alerts and routing trades to cTrader Open API V2",
    version="0.2.0",
)


class WebhookPayload(BaseModel):
    action: Literal["BUY", "SELL", "buy", "sell"] = Field(
        ...,
        description="Trade direction: BUY or SELL",
        examples=["BUY"],
    )
    symbol: str = Field(
        ...,
        description="Trading pair symbol (e.g., XAUUSD, EURUSD)",
        examples=["XAUUSD"],
    )
    volume: float = Field(
        ...,
        gt=0,
        description="Order lot size / volume (e.g., 0.01)",
        examples=[0.01],
    )


# ---------------------------------------------------------------------------
# cTrader Open API Core Functions
# ---------------------------------------------------------------------------
def validate_credentials():
    """Validate that all required cTrader credentials are provided."""
    missing = []
    if not CTRADER_CLIENT_ID or CTRADER_CLIENT_ID == "your_client_id_here":
        missing.append("CTRADER_CLIENT_ID")
    if not CTRADER_CLIENT_SECRET or CTRADER_CLIENT_SECRET == "your_client_secret_here":
        missing.append("CTRADER_CLIENT_SECRET")
    if not CTRADER_ACCOUNT_ID or CTRADER_ACCOUNT_ID == "12345678":
        missing.append("CTRADER_ACCOUNT_ID")
    if not CTRADER_ACCESS_TOKEN or CTRADER_ACCESS_TOKEN == "your_access_token_here":
        missing.append("CTRADER_ACCESS_TOKEN")

    if missing:
        raise ValueError(
            f"Missing or placeholder cTrader credentials in environment: {', '.join(missing)}. "
            "Please configure them in your .env file."
        )


async def send_and_receive(ws, payload_type: int, payload: Dict[str, Any], timeout: float = 10.0) -> Dict[str, Any]:
    """Helper function to send JSON message to cTrader Open API and await the response."""
    msg_id = f"tv_{uuid.uuid4().hex[:8]}"
    request_data = {
        "clientMsgId": msg_id,
        "payloadType": payload_type,
        "payload": payload,
    }

    logger.debug("Sending message [type=%d, id=%s]: %s", payload_type, msg_id, request_data)
    await ws.send(json.dumps(request_data))

    start_time = asyncio.get_event_loop().time()
    while True:
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > timeout:
            raise TimeoutError(f"cTrader Open API response timed out after {timeout}s for message type {payload_type}")

        raw_message = await asyncio.wait_for(ws.recv(), timeout=max(1.0, timeout - elapsed))
        message = json.loads(raw_message)
        logger.debug("Received message: %s", message)

        resp_type = message.get("payloadType")
        resp_payload = message.get("payload", {})

        # Handle API Error Responses
        if resp_type == ProtoOAPayloadType.PROTO_OA_ERROR_RES:
            error_code = resp_payload.get("errorCode", "UNKNOWN_ERROR")
            description = resp_payload.get("description", "No description provided")
            raise RuntimeError(f"cTrader Open API Error [{error_code}]: {description}")

        if resp_type == ProtoOAPayloadType.PROTO_OA_ORDER_ERROR_EVENT:
            error_code = resp_payload.get("errorCode", "ORDER_ERROR")
            description = resp_payload.get("description", "Order rejected")
            raise RuntimeError(f"cTrader Order Error [{error_code}]: {description}")

        # Return matching response or execution event
        if message.get("clientMsgId") == msg_id or resp_type in (
            ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_RES,
            ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_RES,
            ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_RES,
            ProtoOAPayloadType.PROTO_OA_EXECUTION_EVENT,
        ):
            return message


async def resolve_symbol_id(ws, symbol: str, account_id: int) -> int:
    """Resolve symbol name (e.g., 'XAUUSD', 'EUR/USD') to integer symbolId via cTrader Open API."""
    # Check in-memory cache first
    normalized_target = symbol.replace("/", "").replace(".", "").upper()
    if normalized_target in SYMBOL_CACHE:
        return SYMBOL_CACHE[normalized_target]

    # If symbol is already a numeric ID
    if symbol.isdigit():
        return int(symbol)

    logger.info("Fetching symbols list from cTrader for Account ID: %d", account_id)
    response = await send_and_receive(
        ws,
        ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_REQ,
        {
            "ctidTraderAccountId": account_id,
            "includeArchivedSymbols": False,
        },
    )

    symbols_list = response.get("payload", {}).get("symbol", [])
    for sym in symbols_list:
        sym_id = sym.get("symbolId")
        sym_name = sym.get("symbolName") or sym.get("name") or ""
        clean_name = sym_name.replace("/", "").replace(".", "").upper()
        if sym_id:
            SYMBOL_CACHE[clean_name] = sym_id

    if normalized_target in SYMBOL_CACHE:
        return SYMBOL_CACHE[normalized_target]

    # Try partial match if exact match not found
    for name, s_id in SYMBOL_CACHE.items():
        if normalized_target in name or name in normalized_target:
            logger.info("Found fuzzy match for symbol '%s' -> '%s' (ID: %d)", symbol, name, s_id)
            SYMBOL_CACHE[normalized_target] = s_id
            return s_id

    raise ValueError(f"Symbol '{symbol}' not found in cTrader account {account_id}.")


async def execute_ctrader_order(action: str, symbol: str, volume: float) -> Dict[str, Any]:
    """
    Execute Market Order via cTrader Open API V2 (JSON Protocol over WebSocket).
    Uses credentials defined in os.environ (Client ID, Client Secret, Account ID, Access Token).
    """
    try:
        import websockets
    except ImportError:
        raise RuntimeError("websockets library is not installed. Please run: pip install -r requirements.txt")

    validate_credentials()

    account_id = int(CTRADER_ACCOUNT_ID)
    action_upper = action.strip().upper()
    trade_side = ProtoOATradeSide.BUY if action_upper == "BUY" else ProtoOATradeSide.SELL

    # cTrader volume calculation: 1 lot = 100,000 units. Protocol uses 0.01 units (cents).
    # 1.00 lot = 10,000,000 cents | 0.01 lot = 100,000 cents
    volume_cents = int(round(volume * 10_000_000))

    logger.info(
        "Connecting to cTrader Open API [%s] (%s) to execute order: %s %s (volume: %s)",
        CTRADER_ENVIRONMENT,
        CTRADER_WS_URL,
        action_upper,
        symbol,
        volume,
    )

    # SSL context for secure WebSocket
    ssl_context = ssl.create_default_context()

    async with websockets.connect(CTRADER_WS_URL, ssl=ssl_context) as ws:
        # Step 1: Application Authorization
        logger.info("Authorizing Application with Client ID...")
        await send_and_receive(
            ws,
            ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_REQ,
            {
                "clientId": CTRADER_CLIENT_ID,
                "clientSecret": CTRADER_CLIENT_SECRET,
            },
        )
        logger.info("Application authorized successfully.")

        # Step 2: Account Authorization
        logger.info("Authorizing Account ID: %d...", account_id)
        await send_and_receive(
            ws,
            ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_REQ,
            {
                "ctidTraderAccountId": account_id,
                "accessToken": CTRADER_ACCESS_TOKEN,
            },
        )
        logger.info("Account authorized successfully.")

        # Step 3: Resolve Symbol ID
        symbol_id = await resolve_symbol_id(ws, symbol, account_id)
        logger.info("Resolved symbol '%s' to Symbol ID: %d", symbol, symbol_id)

        # Step 4: Submit Market Order Request
        logger.info("Submitting Market Order -> Side: %s, Symbol ID: %d, Volume Cents: %d", action_upper, symbol_id, volume_cents)
        order_response = await send_and_receive(
            ws,
            ProtoOAPayloadType.PROTO_OA_NEW_ORDER_REQ,
            {
                "ctidTraderAccountId": account_id,
                "symbolId": symbol_id,
                "orderType": ProtoOAOrderType.MARKET,
                "tradeSide": trade_side,
                "volume": volume_cents,
            },
            timeout=15.0,
        )

        logger.info("Order successfully submitted to cTrader! Response: %s", order_response)
        return {
            "status": "SUCCESS",
            "environment": CTRADER_ENVIRONMENT,
            "action": action_upper,
            "symbol": symbol,
            "symbolId": symbol_id,
            "volume": volume,
            "volumeCents": volume_cents,
            "response": order_response.get("payload", {}),
        }


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------
@app.get("/", tags=["Health"])
def root():
    return {
        "message": "TradingView to cTrader Webhook Server is running.",
        "environment": CTRADER_ENVIRONMENT,
        "status": "online",
    }


@app.get("/health", tags=["Health"])
def health_check():
    has_creds = bool(
        CTRADER_CLIENT_ID
        and CTRADER_CLIENT_SECRET
        and CTRADER_ACCOUNT_ID
        and CTRADER_ACCESS_TOKEN
        and CTRADER_CLIENT_ID != "your_client_id_here"
    )
    return {
        "status": "healthy",
        "cTraderConfigured": has_creds,
        "environment": CTRADER_ENVIRONMENT,
    }


@app.post("/webhook", status_code=status.HTTP_200_OK, tags=["Webhook"])
async def receive_webhook(payload: WebhookPayload):
    """
    Receive webhook alert from TradingView and execute trade order in cTrader Open API.
    
    Expected JSON Payload:
    {
        "action": "BUY",
        "symbol": "XAUUSD",
        "volume": 0.01
    }
    """
    logger.info("Received TradingView Webhook: %s", payload.model_dump())

    try:
        result = await execute_ctrader_order(
            action=payload.action,
            symbol=payload.symbol,
            volume=payload.volume,
        )
        return {
            "success": True,
            "data": result,
        }
    except ValueError as ve:
        logger.warning("Configuration or validation error: %s", str(ve))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve),
        )
    except Exception as e:
        logger.error("Failed to execute cTrader order: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error executing cTrader order: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host=host, port=port, reload=True)
