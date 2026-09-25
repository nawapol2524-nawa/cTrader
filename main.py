import asyncio
import json
import logging
import os
import ssl
import uuid
from typing import Any, Dict, Literal, Optional

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

# โหลดค่า Environment จากไฟล์ .env หากมี
load_dotenv()

# ---------------------------------------------------------------------------
# 📝 การตั้งค่า Logging แสดงผลชัดเจนทุกขั้นตอน
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("cTrader-OpenAPI-Bot")

# ---------------------------------------------------------------------------
# ⚙️ ดึงค่า Credentials จาก os.environ ตามชื่อตัวแปรที่กำหนดเป๊ะๆ
# ---------------------------------------------------------------------------
ClientID: str = os.environ.get("ClientID", "") or os.environ.get("CTRADER_CLIENT_ID", "")
ClientSecret: str = os.environ.get("ClientSecret", "") or os.environ.get("CTRADER_CLIENT_SECRET", "")
AccessToken: str = os.environ.get("AccessToken", "") or os.environ.get("CTRADER_ACCESS_TOKEN", "")
AccountID: str = os.environ.get("AccountID", "") or os.environ.get("CTRADER_ACCOUNT_ID", "")

# cTrader Open API ฝั่ง Demo:
# สำหรับ JSON over Secure WebSocket ใช้พอร์ตมาตรฐาน 5036
# (หรือสามารถระบุ URL อื่นผ่านตัวแปร CTRADER_WS_URL ได้)
DEFAULT_WS_URL = "wss://demo.ctraderapi.com:5036"
CTRADER_WS_URL: str = os.environ.get("CTRADER_WS_URL", DEFAULT_WS_URL)

# Memory Cache สำหรับ Symbol ID (ป้องกันการดึงรายชื่อคู่เงินซ้ำๆ เพื่อลด Latency)
SYMBOL_CACHE: Dict[str, int] = {}

# ---------------------------------------------------------------------------
# 📦 cTrader Open API Payload Types (Open API Protocol V2 JSON)
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
# 🚀 FastAPI Web Server & Data Models
# ---------------------------------------------------------------------------
app = FastAPI(
    title="TradingView to cTrader Open API Bot",
    description="Webhook Server สำหรับรับคำสั่งจาก TradingView และส่งตรงไปยัง cTrader Open API V2",
    version="1.0.0",
)


class WebhookPayload(BaseModel):
    action: Literal["BUY", "SELL", "buy", "sell"] = Field(
        ...,
        description="ทิศทางการเทรด: BUY หรือ SELL",
        examples=["BUY"],
    )
    symbol: str = Field(
        ...,
        description="ชื่อคู่เงินหรือสินทรัพย์ เช่น GBPUSD, XAUUSD",
        examples=["GBPUSD"],
    )
    volume: float = Field(
        ...,
        gt=0,
        description="ขนาดคำสั่ง (Lot Size) เช่น 0.01",
        examples=[0.01],
    )


# ---------------------------------------------------------------------------
# 🛠️ Helper Functions สำหรับสื่อสารกับ cTrader Open API
# ---------------------------------------------------------------------------
def check_credentials():
    """ตรวจสอบความพร้อมของ Credentials"""
    missing = []
    if not ClientID:
        missing.append("ClientID")
    if not ClientSecret:
        missing.append("ClientSecret")
    if not AccessToken:
        missing.append("AccessToken")
    if not AccountID:
        missing.append("AccountID")

    if missing:
        err_msg = f"❌ ขาดค่า Credentials: {', '.join(missing)} ใน Environment Variables"
        logger.error(err_msg)
        raise ValueError(err_msg)


async def send_and_wait_response(ws, payload_type: int, payload: Dict[str, Any], timeout: float = 12.0) -> Dict[str, Any]:
    """ส่งข้อความ JSON-RPC ไปยัง cTrader Open API และรอรับ Response ที่ตรงกัน"""
    msg_id = f"req_{uuid.uuid4().hex[:8]}"
    request_msg = {
        "clientMsgId": msg_id,
        "payloadType": payload_type,
        "payload": payload,
    }

    await ws.send(json.dumps(request_msg))
    logger.debug("📤 ส่งข้อความ [Type: %d, ID: %s]", payload_type, msg_id)

    start_time = asyncio.get_event_loop().time()
    while True:
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > timeout:
            raise TimeoutError(f"cTrader Open API ไม่ตอบสนองภายใน {timeout} วินาที (PayloadType: {payload_type})")

        raw_data = await asyncio.wait_for(ws.recv(), timeout=max(1.0, timeout - elapsed))
        response = json.loads(raw_data)
        resp_type = response.get("payloadType")
        resp_payload = response.get("payload", {})

        # ตรวจสอบ Error Response จาก cTrader
        if resp_type == ProtoOAPayloadType.PROTO_OA_ERROR_RES:
            error_code = resp_payload.get("errorCode", "UNKNOWN_ERROR")
            desc = resp_payload.get("description", "ไม่ทราบสาเหตุ")
            raise RuntimeError(f"cTrader Open API ปฏิเสธคำขอ [{error_code}]: {desc}")

        if resp_type == ProtoOAPayloadType.PROTO_OA_ORDER_ERROR_EVENT:
            error_code = resp_payload.get("errorCode", "ORDER_ERROR")
            desc = resp_payload.get("description", "คำสั่งเปิดออเดอร์ถูกปฏิเสธ")
            raise RuntimeError(f"cTrader Order Error [{error_code}]: {desc}")

        # ตอบกลับเมื่อตรงกับ Request ID หรือเป็น Response ของขั้นตอนนั้นๆ
        if response.get("clientMsgId") == msg_id or resp_type in (
            ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_RES,
            ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_RES,
            ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_RES,
            ProtoOAPayloadType.PROTO_OA_EXECUTION_EVENT,
        ):
            return response


async def get_symbol_id(ws, symbol: str, account_id: int) -> int:
    """แปลงชื่อ Symbol (เช่น GBPUSD) ให้เป็น Symbol ID (ตัวเลข) ของ cTrader"""
    clean_target = symbol.replace("/", "").replace(".", "").upper()

    # ตรวจสอบใน Memory Cache ก่อน
    if clean_target in SYMBOL_CACHE:
        return SYMBOL_CACHE[clean_target]

    if symbol.isdigit():
        return int(symbol)

    logger.info("🔍 กำลังค้นหา Symbol ID สำหรับ: %s ในบัญชี %d...", symbol, account_id)
    response = await send_and_wait_response(
        ws,
        ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_REQ,
        {
            "ctidTraderAccountId": account_id,
            "includeArchivedSymbols": False,
        },
    )

    symbols_list = response.get("payload", {}).get("symbol", [])
    for sym in symbols_list:
        s_id = sym.get("symbolId")
        s_name = sym.get("symbolName") or sym.get("name") or ""
        normalized = s_name.replace("/", "").replace(".", "").upper()
        if s_id:
            SYMBOL_CACHE[normalized] = s_id

    if clean_target in SYMBOL_CACHE:
        found_id = SYMBOL_CACHE[clean_target]
        logger.info("✅ พบ Symbol ID สำหรับ '%s' คือ: %d", symbol, found_id)
        return found_id

    # Fuzzy match กรณีชื่อใกล้เคียง
    for name, s_id in SYMBOL_CACHE.items():
        if clean_target in name or name in clean_target:
            logger.info("✅ พบ Symbol ID (ใกล้เคียง) สำหรับ '%s' -> '%s': %d", symbol, name, s_id)
            SYMBOL_CACHE[clean_target] = s_id
            return s_id

    raise ValueError(f"ไม่พบสัญลักษณ์ '{symbol}' ในบัญชี cTrader นี้ (กรุณาตรวจสอบชื่อสัญลักษณ์)")


# ---------------------------------------------------------------------------
# 🚀 ฟังก์ชันหลักในการเปิดออเดอร์ cTrader Open API แบบครบวงจร (Async)
# ---------------------------------------------------------------------------
async def execute_ctrader_order(action: str, symbol: str, volume: float) -> Dict[str, Any]:
    """
    เชื่อมต่อ cTrader Open API -> App Auth -> Account Auth -> New Order -> Close Connection
    """
    try:
        import websockets
    except ImportError:
        raise RuntimeError("กรุณาติดตั้ง websockets ด้วยคำสั่ง pip install -r requirements.txt")

    check_credentials()

    account_id_int = int(AccountID)
    action_upper = action.strip().upper()
    trade_side = ProtoOATradeSide.BUY if action_upper == "BUY" else ProtoOATradeSide.SELL

    # ใน cTrader Open API:
    # 1.00 Lot (Forex/Gold มาตรฐาน) = 100,000 Units
    # API ใช้หน่วย cents (0.01 ของ unit)
    # ดังนั้น 1.00 Lot = 10,000,000 cents | 0.01 Lot = 100,000 cents
    volume_cents = int(round(volume * 10_000_000))

    logger.info("==================================================")
    logger.info("🔄 [1/5] เริ่มต้นเชื่อมต่อไปยัง cTrader Open API ฝั่ง Demo (%s)...", CTRADER_WS_URL)

    ssl_context = ssl.create_default_context()

    # เปิดการเชื่อมต่อแบบ Async Context Manager (จะปิดการเชื่อมต่อโดยอัตโนมัติเมื่อสิ้นสุด)
    async with websockets.connect(CTRADER_WS_URL, ssl=ssl_context) as ws:
        logger.info("🌐 เชื่อมต่อ Socket สำเร็จ!")

        try:
            # 1. App Authorization
            logger.info("🔑 [2/5] กำลังทำ Application Authorization ด้วย ClientID...")
            app_auth_res = await send_and_wait_response(
                ws,
                ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_REQ,
                {
                    "clientId": ClientID,
                    "clientSecret": ClientSecret,
                },
            )
            logger.info("✅ [2/5] Application Authorization สำเร็จ!")

            # 2. Account Authorization
            logger.info("👤 [3/5] กำลังทำ Account Authorization สำหรับ AccountID: %d...", account_id_int)
            acc_auth_res = await send_and_wait_response(
                ws,
                ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_REQ,
                {
                    "ctidTraderAccountId": account_id_int,
                    "accessToken": AccessToken,
                },
            )
            logger.info("✅ [3/5] Account Authorization สำเร็จ!")

            # 3. Resolve Symbol ID
            symbol_id = await get_symbol_id(ws, symbol, account_id_int)

            # 4. ส่งคำสั่ง Market Order (ProtoOANewOrderReq)
            logger.info(
                "📤 [4/5] กำลังส่งคำสั่ง ProtoOANewOrderReq (Market Order): %s %s (Lot: %.2f | Volume Cents: %d)...",
                action_upper,
                symbol,
                volume,
                volume_cents,
            )
            order_req_payload = {
                "ctidTraderAccountId": account_id_int,
                "symbolId": symbol_id,
                "orderType": ProtoOAOrderType.MARKET,
                "tradeSide": trade_side,
                "volume": volume_cents,
            }
            order_response = await send_and_wait_response(
                ws,
                ProtoOAPayloadType.PROTO_OA_NEW_ORDER_REQ,
                order_req_payload,
                timeout=15.0,
            )

            # 5. รับ Response ยืนยันการเปิดออเดอร์
            resp_payload = order_response.get("payload", {})
            logger.info("🎉 [5/5] ได้รับการยืนยันการเปิดออเดอร์สำเร็จจาก cTrader!")
            logger.info("📄 ผลลัพธ์จากเซิร์ฟเวอร์: %s", json.dumps(resp_payload))

            return {
                "status": "SUCCESS",
                "action": action_upper,
                "symbol": symbol,
                "symbolId": symbol_id,
                "volume": volume,
                "volumeCents": volume_cents,
                "orderResponse": resp_payload,
            }

        finally:
            logger.info("🔒 กำลังปิดการเชื่อมต่อ cTrader Open API อย่างสมบูรณ์...")

    logger.info("🔌 ปิดการเชื่อมต่อเรียบร้อยแล้ว")
    logger.info("==================================================")


# ---------------------------------------------------------------------------
# 📥 API Endpoints
# ---------------------------------------------------------------------------
@app.post("/webhook", status_code=status.HTTP_200_OK, tags=["Webhook"])
async def receive_webhook(payload: WebhookPayload):
    """
    รับ Webhook จาก TradingView
    JSON Payload ตัวอย่าง:
    {
        "action": "BUY",
        "symbol": "GBPUSD",
        "volume": 0.01
    }
    """
    logger.info("🔔 ได้รับสัญญาณ TradingView Webhook: Action=%s, Symbol=%s, Volume=%s", payload.action, payload.symbol, payload.volume)

    try:
        result = await execute_ctrader_order(
            action=payload.action,
            symbol=payload.symbol,
            volume=payload.volume,
        )
        return {
            "success": True,
            "message": f"Successfully executed {payload.action} {payload.symbol}",
            "data": result,
        }

    except ValueError as ve:
        logger.error("❌ ข้อมูลไม่ถูกต้องหรือขาดการตั้งค่า: %s", str(ve))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))

    except Exception as e:
        logger.error("❌ เกิดข้อผิดพลาดในการส่งออเดอร์ cTrader: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"cTrader Open API Error: {str(e)}",
        )


@app.get("/", tags=["Health"])
def health_check():
    return {
        "status": "online",
        "service": "TradingView to cTrader Open API Bot",
        "configured": bool(ClientID and ClientSecret and AccessToken and AccountID),
        "endpoint": CTRADER_WS_URL,
    }


# ---------------------------------------------------------------------------
# 🏁 Start Server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # รองรับการรันทั้ง Local และ Render โดยอ่าน PORT อัตโนมัติ (Default 10000)
    port = int(os.environ.get("PORT", 10000))
    logger.info("📡 กำลังเริ่มการทำงาน Webhook Server ที่พอร์ต %d...", port)
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
