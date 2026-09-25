import asyncio
import json
import logging
import os
import ssl
import uuid
from typing import Any, Dict, Literal, Optional

import uvicorn
import websockets
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 📝 การตั้งค่า Logging ภาษาไทยแสดงผลชัดเจนทุกขั้นตอน
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("cTrader-OpenAPI-Bot")

# ---------------------------------------------------------------------------
# ⚙️ ดึงค่า Credentials จาก os.environ ตามชื่อตัวแปรที่กำหนด
# ---------------------------------------------------------------------------
ClientID: str = os.environ.get("ClientID", "") or os.environ.get("CTRADER_CLIENT_ID", "")
ClientSecret: str = os.environ.get("ClientSecret", "") or os.environ.get("CTRADER_CLIENT_SECRET", "")
AccessToken: str = os.environ.get("AccessToken", "") or os.environ.get("CTRADER_ACCESS_TOKEN", "")
AccountID: str = os.environ.get("AccountID", "") or os.environ.get("CTRADER_ACCOUNT_ID", "")

# Endpoint WebSocket ของ cTrader Demo พอร์ต 5036 (JSON Protocol)
CTRADER_WS_URL: str = os.environ.get("CTRADER_WS_URL", "wss://demo.ctraderapi.com:5036")

# Memory Cache สำหรับจับคู่ชื่อคู่เงิน -> Symbol ID (ช่วยลด Latency)
SYMBOL_CACHE: Dict[str, int] = {}

# ---------------------------------------------------------------------------
# 📦 cTrader Open API Protocol V2 Payload Types
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
    PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ = 2149
    PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES = 2150


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
    description="Webhook Server รับสัญญาณ TradingView และส่งคำสั่งตรงเข้า cTrader Open API V2",
    version="2.1.0",
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
    """ตรวจสอบความพร้อมของ Credentials ที่ได้รับจาก Environment"""
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
        error_msg = f"❌ ขาดค่า Credentials สำคัญ: {', '.join(missing)} ใน Environment Variables"
        logger.error(error_msg)
        raise ValueError(error_msg)


async def send_and_wait_response(ws, payload_type: int, payload: Dict[str, Any], timeout: float = 12.0) -> Dict[str, Any]:
    """ส่งข้อความ JSON ไปยัง cTrader Open API และรอรับ Response ที่ตรงกัน"""
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
            ProtoOAPayloadType.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES,
            ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_RES,
            ProtoOAPayloadType.PROTO_OA_EXECUTION_EVENT,
        ):
            return response


async def resolve_ctid_account_id(ws, target_account_str: str, access_token: str) -> int:
    """
    ดึงรายการบัญชีทั้งหมดที่ผูกกับ AccessToken และแปลงให้เป็น ctidTraderAccountId ที่ถูกต้อง
    (รองรับทั้งผู้ใช้ที่ใส่ ctidTraderAccountId หรือใส่เลข Login/พอร์ต 2548625)
    """
    logger.info("🔍 [สเต็ป 3.1] ดึงรายการบัญชีทั้งหมดที่ผูกกับ AccessToken...")
    resp = await send_and_wait_response(
        ws,
        ProtoOAPayloadType.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        {"accessToken": access_token},
    )

    accounts = resp.get("payload", {}).get("ctidTraderAccount", [])
    target_clean = str(target_account_str).strip()

    if not accounts:
        logger.warning("⚠️ ไม่พบข้อมูลรายการบัญชีใน Token ใช้ค่าเดิม: %s", target_clean)
        return int(target_clean)

    logger.info("📋 พบบัญชีใน Access Token ทั้งหมด %d บัญชี:", len(accounts))
    matched_id = None

    for acc in accounts:
        ctid_id = acc.get("ctidTraderAccountId")
        trader_login = str(acc.get("traderLogin", ""))
        is_live = acc.get("isLive", False)
        env_type = "Live" if is_live else "Demo"
        logger.info("   👉 ctidTraderAccountId: %s | Login ID: %s | ประเภท: %s", ctid_id, trader_login, env_type)

        # ตรวจสอบว่าตรงกับ ctidTraderAccountId หรือ traderLogin
        if str(ctid_id) == target_clean or trader_login == target_clean:
            matched_id = ctid_id

    if matched_id:
        logger.info("🎯 พบการจับคู่บัญชีสำเร็จ! ใช้ ctidTraderAccountId: %d", matched_id)
        return int(matched_id)

    # หากมีบัญชีเดียวใน Token ให้เลือกใช้บัญชีนั้นโดยอัตโนมัติ
    if len(accounts) == 1:
        auto_id = accounts[0].get("ctidTraderAccountId")
        logger.info("💡 ตรวจพบบัญชีเดียวใน Token -> เลือกใช้ ctidTraderAccountId: %d อัตโนมัติ", auto_id)
        return int(auto_id)

    logger.warning("⚠️ ไม่พบ ID ที่ระบุ (%s) ในรายการบัญชี จะลองใช้ค่านั้นโดยตรง", target_clean)
    return int(target_clean)


async def get_symbol_id(ws, symbol: str, account_id: int) -> int:
    """แปลงชื่อ Symbol (เช่น GBPUSD) ให้เป็น Symbol ID (ตัวเลข) ของ cTrader"""
    clean_target = symbol.replace("/", "").replace(".", "").upper()

    if clean_target in SYMBOL_CACHE:
        return SYMBOL_CACHE[clean_target]

    if symbol.isdigit():
        return int(symbol)

    logger.info("🔍 [สเต็ป 4.1] ค้นหา Symbol ID สำหรับ %s ในบัญชี %d...", symbol, account_id)
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
        logger.info("✅ [สเต็ป 4.2] พบ Symbol ID สำหรับ '%s' คือ: %d", symbol, found_id)
        return found_id

    for name, s_id in SYMBOL_CACHE.items():
        if clean_target in name or name in clean_target:
            logger.info("✅ [สเต็ป 4.2] พบ Symbol ID ใกล้เคียงสำหรับ '%s' -> '%s': %d", symbol, name, s_id)
            SYMBOL_CACHE[clean_target] = s_id
            return s_id

    raise ValueError(f"ไม่พบสัญลักษณ์ '{symbol}' ในบัญชี cTrader นี้ (กรุณาตรวจสอบชื่อสัญลักษณ์)")


# ---------------------------------------------------------------------------
# 🚀 ฟังก์ชันหลัก: เปิด WebSocket -> App Auth -> Acc Auth -> New Order -> Close
# ---------------------------------------------------------------------------
async def execute_ctrader_order(action: str, symbol: str, volume: float) -> Dict[str, Any]:
    """
    เชื่อมต่อ WebSocket -> App Auth -> Account Auth -> ส่ง Market Order -> รับผลลัพธ์ -> ปิด Connection
    """
    check_credentials()

    action_upper = action.strip().upper()
    trade_side = ProtoOATradeSide.BUY if action_upper == "BUY" else ProtoOATradeSide.SELL

    # ใน cTrader Open API:
    # 1.00 Lot (Forex/Gold มาตรฐาน) = 100,000 Units
    # Protocol ใช้หน่วย cents (0.01 ของ unit)
    # 1.00 Lot = 10,000,000 cents | 0.01 Lot = 100,000 cents
    volume_cents = int(round(volume * 10_000_000))

    logger.info("==================================================")
    logger.info("🔄 [สเต็ป 1/5] กำลังเปิดการเชื่อมต่อ WebSocket ไปยัง %s...", CTRADER_WS_URL)

    ssl_context = ssl.create_default_context()

    # เปิด WebSocket ผ่าน Async Context Manager (รับประกันว่าจะปิด Connection เสมอเมื่อเสร็จสิ้น)
    async with websockets.connect(CTRADER_WS_URL, ssl=ssl_context, open_timeout=10.0) as ws:
        logger.info("🌐 [สเต็ป 1/5] เชื่อมต่อ WebSocket สำเร็จ!")

        try:
            # 1. Application Authorization
            logger.info("🔑 [สเต็ป 2/5] กำลังส่ง App Authorization (ClientID: %s)...", ClientID[:6] + "..." if len(ClientID) > 6 else ClientID)
            await send_and_wait_response(
                ws,
                ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_REQ,
                {
                    "clientId": ClientID,
                    "clientSecret": ClientSecret,
                },
            )
            logger.info("✅ [สเต็ป 2/5] App Authorization สำเร็จ!")

            # 2. ค้นหา ctidTraderAccountId ที่ถูกต้อง
            target_account_id = await resolve_ctid_account_id(ws, AccountID, AccessToken)

            # 3. Account Authorization
            logger.info("👤 [สเต็ป 3/5] กำลังส่ง Account Authorization สำหรับ ctidTraderAccountId: %d...", target_account_id)
            await send_and_wait_response(
                ws,
                ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_REQ,
                {
                    "ctidTraderAccountId": target_account_id,
                    "accessToken": AccessToken,
                },
            )
            logger.info("✅ [สเต็ป 3/5] Account Authorization สำเร็จ!")

            # 4. ตรวจสอบ Symbol ID
            symbol_id = await get_symbol_id(ws, symbol, target_account_id)

            # 5. ส่งคำสั่ง Market Order (ProtoOANewOrderReq)
            logger.info(
                "📤 [สเต็ป 4/5] กำลังส่งคำสั่ง ProtoOANewOrderReq (Market Order): %s %s (Lot: %.2f | Cents: %d)...",
                action_upper,
                symbol,
                volume,
                volume_cents,
            )
            order_req_payload = {
                "ctidTraderAccountId": target_account_id,
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

            # 6. ดึงผลลัพธ์การเปิดออเดอร์
            resp_payload = order_response.get("payload", {})
            logger.info("🎉 [สเต็ป 5/5] ได้รับการยืนยันการเปิดออเดอร์สำเร็จจาก cTrader!")
            logger.info("📄 ผลลัพธ์: %s", json.dumps(resp_payload))

            return {
                "status": "SUCCESS",
                "action": action_upper,
                "symbol": symbol,
                "symbolId": symbol_id,
                "accountId": target_account_id,
                "volume": volume,
                "volumeCents": volume_cents,
                "orderResult": resp_payload,
            }

        finally:
            logger.info("🔒 กำลังปิดการเชื่อมต่อ WebSocket cTrader อย่างสมบูรณ์...")

    logger.info("🔌 ปิดการเชื่อมต่อเรียบร้อยแล้ว ไม่มีการค้างเซสชัน")
    logger.info("==================================================")


# ---------------------------------------------------------------------------
# 📥 API Endpoints
# ---------------------------------------------------------------------------
@app.post("/webhook", status_code=status.HTTP_200_OK, tags=["Webhook"])
async def receive_webhook(payload: WebhookPayload):
    """
    รับ Webhook สัญญาณเทรดจาก TradingView
    โครงสร้าง JSON ตัวอย่าง:
    {
        "action": "BUY",
        "symbol": "GBPUSD",
        "volume": 0.01
    }
    """
    logger.info("🔔 [Webhook เข้ามา] Action=%s, Symbol=%s, Volume=%s", payload.action, payload.symbol, payload.volume)

    try:
        result = await execute_ctrader_order(
            action=payload.action,
            symbol=payload.symbol,
            volume=payload.volume,
        )
        return {
            "success": True,
            "message": f"เปิดออเดอร์ {payload.action.upper()} {payload.symbol} สำเร็จใน cTrader",
            "data": result,
        }

    except ValueError as ve:
        logger.error("❌ การตั้งค่าไม่ถูกต้อง: %s", str(ve))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))

    except Exception as e:
        logger.error("❌ เกิดข้อผิดพลาดในการยิงออเดอร์ cTrader: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"cTrader Open API Error: {str(e)}",
        )


@app.get("/", tags=["Health"])
def health_check():
    return {
        "status": "online",
        "service": "TradingView to cTrader Open API Bot",
        "endpoint": CTRADER_WS_URL,
        "credentials_configured": bool(ClientID and ClientSecret and AccessToken and AccountID),
    }


# ---------------------------------------------------------------------------
# 🏁 Start Server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    logger.info("📡 เริ่มทำงาน Webhook Server ที่พอร์ต %d...", port)
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
