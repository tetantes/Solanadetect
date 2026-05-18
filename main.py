import asyncio
import json
import logging
import os
import httpx
from fastapi import FastAPI
from websockets import connect

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "healthy", "service": "Live Mint Radar Active"}

# Target System Keys
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8317307510:AAHqbY63j5vlt70WaaFZGe2We40E-E_fMyM")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "6011460052")
RPC_WSS_URL = os.getenv("RPC_WSS_URL")  # MUST use wss:// instead of https://

RAYDIUM_LP_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

async def send_telegram_alert(client: httpx.AsyncClient, token_address: str, signature: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # The <code> tags around the token address make it click-to-copy in Telegram
    alert_msg = (
        f"🚨 <b>NEW SOLANA LAUNCH DETECTED</b> 🚨\n\n"
        f"📃 <b>Mint/CA:</b>\n<code>{token_address}</code>\n\n"
        f"🔗 <a href='https://dexscreener.com/solana/{token_address}'>DexScreener</a> | "
        f"<a href='https://rugcheck.xyz/tokens/{token_address}'>RugCheck</a>\n"
        f"📦 <a href='https://solscan.io/tx/{signature}'>Tx Log Signature</a>"
    )
    
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": alert_msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        await client.post(url, json=payload, timeout=5.0)
        logger.info(f"✨ Instant Alert Sent for: {token_address}")
    except Exception as e:
        logger.error(f"Telegram Alert Route Blocked: {e}")

async def process_transaction(client: httpx.AsyncClient, signature: str, rpc_http_url: str):
    """Fetches full transaction payload block details to isolate the Mint CA"""
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
            ]
        }
        response = await client.post(rpc_http_url, json=payload, timeout=10.0)
        if response.status_code != 200: return

        tx_data = response.json().get("result", {})
        if not tx_data: return

        meta = tx_data.get("meta", {})
        post_balances = meta.get("postTokenBalances", [])
        
        for balance in post_balances:
            mint = balance.get("mint")
            # Filter out Native WSOL wrapping contract address
            if mint and mint != "So11111111111111111111111111111111111111112":
                await send_telegram_alert(client, mint, signature)
                break
    except Exception as e:
        logger.error(f"Extraction processing anomaly on tx {signature}: {e}")

async def listen_to_blockchain():
    if not RPC_WSS_URL:
        logger.error("CRITICAL ERROR: RPC_WSS_URL environment variable is empty!")
        return

    rpc_http_url = RPC_WSS_URL.replace("wss://", "https://")

    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(limits=limits) as httpx_client:
        while True:
            try:
                logger.info("Connecting to Solana WebSocket Stream...")
                async with connect(RPC_WSS_URL) as websocket:
                    subscription_payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [RAYDIUM_LP_V4]},
                            {"commitment": "processed"}
                        ]
                    }
                    await websocket.send(json.dumps(subscription_payload))
                    logger.info("Subscription Active: Stream locked on Raydium LP program.")

                    async for message in websocket:
                        data = json.loads(message)
                        logs = data.get("params", {}).get("result", {}).get("value", {}).get("logs", [])
                        
                        if any("initialize2: InitializeInstruction" in log for log in logs):
                            signature = data.get("params", {}).get("result", {}).get("value", {}).get("signature")
                            logger.info(f"Detected launch pool block signature: {signature}")
                            
                            asyncio.create_task(process_transaction(httpx_client, signature, rpc_http_url))

            except Exception as e:
                logger.error(f"WebSocket interface dropped connection: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(listen_to_blockchain())
                            
