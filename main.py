import os
import uvicorn
import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import logging

# ตั้งค่า Logging ให้แสดงผลชัดเจนใน Console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

# ---------------------------------------------------------------------------
# ⚙️ 1. ตั้งค่าเชื่อมต่อ cTrader Remote MCP
# ---------------------------------------------------------------------------
# แนะนำให้ตั้งค่า CTRADER_MCP_TOKEN ใน Wispbyte (แท็บ Variables/Environment)
# หากไม่ได้ตั้งค่า ให้แทนที่คำว่า "ใส่_TOKEN_ยาวๆ_ของคุณที่นี่" ด้วย Token จากหน้าจอ cTrader
BEARER_TOKEN = os.environ.get("CTRADER_MCP_TOKEN", "eyJwbGFudCI6ImRlcml2IiwiZW52aXJvbm1lbnQiOiJkZW1vIiwidG9rZW4iOiJXKzBMK01aZThrL3NRUHd5VVZkc3N1cVRCNDRvWEdZQ1FJYk9kWkZWNDEwPSJ9")
MCP_URL = "https://mcp.ctrader.com/trading/mcp"

# โครงสร้างข้อมูลที่คาดว่าจะได้รับจาก TradingView
class WebhookPayload(BaseModel):
    action: str      # "BUY" หรือ "SELL"
    symbol: str      # "XAUUSD" หรือ "GBPUSD"
    volume: float    # ขนาด Lot เช่น 0.01

# ---------------------------------------------------------------------------
# 🚀 2. ฟังก์ชันหลักสำหรับยิงออเดอร์เข้า cTrader MCP
# ---------------------------------------------------------------------------
async def execute_mcp_order(action: str, symbol: str, volume: float):
    if BEARER_TOKEN == "ใส่_TOKEN_ยาวๆ_ของคุณที่นี่":
        logger.error("❌ ยังไม่ได้ใส่ Bearer Token! โปรดแก้ไขใน main.py หรือตั้งค่า Environment")
        return False

    # เตรียม Headers
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # คำนวณ Volume: cTrader MCP มักใช้หน่วยเป็น Units
    # (สมมติฐาน: 1 Lot = 100,000 Units สำหรับ Forex/Gold ส่วนใหญ่)
    # *คุณอาจต้องปรับตัวคูณนี้หากเทรดคริปโตหรือหุ้น
    units = int(volume * 100000) 
    
    # ปรับ Format ของ Action ให้ตรงกับที่ระบบต้องการ ("Buy" หรือ "Sell")
    trade_type = "Buy" if action.upper() == "BUY" else "Sell"

    # โครงสร้างคำสั่ง JSON-RPC สำหรับ Remote MCP
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "create_market_order",
        "params": {
            "symbol": symbol,
            "tradeType": trade_type,
            "volume": units
        }
    }

    logger.info(f"📤 กำลังส่งคำสั่งไป cTrader: {trade_type} {symbol} จำนวน {units} Units")
    
    # ยิง Request ไปยังเซิร์ฟเวอร์ของ cTrader
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(MCP_URL, headers=headers, json=payload, timeout=10.0)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"✅ สำเร็จ! ผลลัพธ์จาก cTrader: {result}")
                return True
            else:
                logger.error(f"❌ cTrader ปฏิเสธคำสั่ง! Status: {response.status_code}, แจ้งเตือน: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ เกิดข้อผิดพลาดในการเชื่อมต่อ cTrader: {str(e)}")
            return False

# ---------------------------------------------------------------------------
# 📥 3. API Endpoints
# ---------------------------------------------------------------------------
@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload):
    try:
        logger.info(f"🔔 TradingView แจ้งเตือนเข้ามา: {payload.action} {payload.symbol} Lot: {payload.volume}")
        
        # นำสัญญาณที่ได้ไปสั่งรัน cTrader
        success = await execute_mcp_order(payload.action, payload.symbol, payload.volume)
        
        if success:
            return {"status": "success", "message": f"Order {payload.action} executed for {payload.symbol}"}
        else:
            raise HTTPException(status_code=500, detail="cTrader execution failed (Check server logs)")
            
    except Exception as e:
        logger.error(f"❌ Webhook Error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/")
def health_check():
    return {"status": "online", "message": "Wispbyte Trading Bot + cTrader MCP is running 🚀"}

# ---------------------------------------------------------------------------
# 🏁 4. คำสั่ง Start Server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # ดึง Port อัตโนมัติจากระบบ Wispbyte (ถ้าไม่มีใช้ 9848)
    port = int(os.environ.get("SERVER_PORT", 9848))
    logger.info(f"📡 บอทพร้อมทำงานที่พอร์ต {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
