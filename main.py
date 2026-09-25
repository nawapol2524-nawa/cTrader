import logging
from typing import Literal
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("TradingView-cTrader")

app = FastAPI(
    title="TradingView to cTrader Webhook Server",
    description="Webhook server for receiving TradingView alerts and routing trades to cTrader (Deriv Demo)",
    version="0.1.0",
)


class WebhookPayload(BaseModel):
    action: Literal["BUY", "SELL", "buy", "sell"] = Field(
        ...,
        description="Trade direction: BUY or SELL",
        examples=["BUY"],
    )
    symbol: str = Field(
        ...,
        description="Trading pair symbol (e.g., XAUUSD, Volatility 75 Index)",
        examples=["XAUUSD"],
    )
    volume: float = Field(
        ...,
        gt=0,
        description="Order lot size / volume (e.g., 0.01)",
        examples=[0.01],
    )


def execute_ctrader_order(action: str, symbol: str, volume: float) -> dict:
    """
    Mock-up function for executing orders via cTrader Open API.
    In production, this function will interact with the cTrader Open API (Fix API / Protobuf / REST / WebSocket)
    to place market or limit orders on the Deriv Demo account.
    """
    normalized_action = action.upper()
    logger.info(
        "[MOCK cTrader Open API] Executing order -> Action: %s, Symbol: %s, Volume: %s",
        normalized_action,
        symbol,
        volume,
    )

    # Simulated response
    mock_order_result = {
        "status": "MOCK_SUCCESS",
        "broker": "Deriv (Demo)",
        "action": normalized_action,
        "symbol": symbol,
        "volume": volume,
        "order_id": "MOCK-CTR-12345678",
        "message": "Order successfully received and routed to cTrader Open API (Simulation)",
    }
    return mock_order_result


@app.get("/", tags=["Health"])
def root():
    return {
        "message": "TradingView to cTrader Webhook Server is running.",
        "status": "online",
    }


@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "healthy"}


@app.post("/webhook", status_code=status.HTTP_200_OK, tags=["Webhook"])
async def receive_webhook(payload: WebhookPayload):
    """
    Receive webhook alert from TradingView and execute trade order in cTrader.
    
    Expected Payload:
    {
        "action": "BUY",
        "symbol": "XAUUSD",
        "volume": 0.01
    }
    """
    logger.info("Received TradingView Webhook: %s", payload.model_dump())

    try:
        result = execute_ctrader_order(
            action=payload.action,
            symbol=payload.symbol,
            volume=payload.volume,
        )
        return {
            "success": True,
            "data": result,
        }
    except Exception as e:
        logger.error("Failed to execute cTrader order: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error executing order: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
