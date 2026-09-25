import os
import uvicorn
import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

# ---------------------------------------------------------------------------
# ⚙️ 1. ตั้งค่าเชื่อมต่อ cTrader Remote MCP
# ---------------------------------------------------------------------------
MCP_URL = "https://mcp.ctrader.com/trading/mcp"
# ดึง Token จาก Environment Variables ของ Render หากไม่มีให้ใส่ตรงนี้เพื่อทดสอบ
BEARER_TOKEN = os.environ.get(
    "CTRADER_MCP_TOKEN", 
    "eyJwbGFudCI6ImRlcml2IiwiZW52aXJvbm1lbnQiOiJkZW1vIiwidG9rZW4iOiJXKzBMK01aZThrL3NRUHd5VVZkc3N1cVRCNDRvWEdZQ1FJYk9kWkZWNDEwPSJ9"
)

class WebhookPayload(BaseModel):
    action: str      
    symbol: str      
    volume: float    

# ---------------------------------------------------------------------------
# 🚀 2. ฟังก์ชันยิงออเดอร์เข้า cTrader MCP
# ---------------------------------------------------------------------------
async def execute_mcp_order(action: str, symbol: str, volume: float):
    if not BEARER_TOKEN or BEARER_TOKEN == "ใส่_TOKEN_ยาวๆ_จาก_cTRADER_ที่นี่":
        logger.error("❌ ยังไม่ได้ตั้งค่า CTRADER_MCP_TOKEN!")
        return False

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    units = int(volume * 100000) 
    trade_type = "Buy" if action.upper() == "BUY" else "Sell"

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
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(MCP_URL, headers=headers, json=payload, timeout=10.0)
            
            if response.status_code == 200:
                result = response.json()
                if "error" in result:
                     logger.error(f"❌ cTrader ส่ง Error กลับมา: {result['error']}")
                     return False
                logger.info(f"✅ สำเร็จ! ผลลัพธ์: {result}")
                return True
            else:
                logger.error(f"❌ HTTP Error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ เกิดข้อผิดพลาดในการส่งข้อมูล: {str(e)}")
            return False

# ---------------------------------------------------------------------------
# 📥 3. API Endpoints
# ---------------------------------------------------------------------------
@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload):
    try:
        logger.info(f"🔔 TradingView แจ้งเตือนเข้ามา: {payload.action} {payload.symbol} Lot: {payload.volume}")
        success = await execute_mcp_order(payload.action, payload.symbol, payload.volume)
        
        if success:
            return {"status": "success", "message": f"Order {payload.action} executed for {payload.symbol}"}
        else:
            raise HTTPException(status_code=500, detail="cTrader MCP execution failed")
            
    except Exception as e:
        logger.error(f"❌ Webhook Error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/")
def health_check():
    return {"status": "online", "message": "Render Trading Bot + cTrader MCP is running 🚀"}

# ---------------------------------------------------------------------------
# 🏁 4. คำสั่ง Start Server สำหรับ Render
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Render จะบังคับส่งหมายเลข PORT มาให้ใน Environment อัตโนมัติ
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"📡 บอทพร้อมทำงานที่พอร์ต {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
