import asyncio
import logging
import sqlite3
import time
import os
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI

# --- FASTAPI SETUP FOR KOYEB FREE TIER ---
app = FastAPI()

@app.get("/")
def home():
    # This keeps Koyeb's health checks happy!
    return {"status": "healthy", "bot": "Solana Tracker Running"}

# --- CONFIGURATION ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8317307510:AAHqbY63j5vlt70WaaFZGe2We40E-E_fMyM")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "6011460052")
CHECK_INTERVAL = 5

DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_TOKEN_BASE_URL = "https://api.dexscreener.com/tokens/v1/solana"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class TokenTrackerBot:
    def __init__(self):
        self.db_path = "processed_tokens.db"
        self.sent_contracts = set()
        self.init_db()
        self.load_sent_contracts()
        
    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    contract_address TEXT PRIMARY KEY,
                    timestamp INTEGER
                )
            """)
            conn.commit()

    def load_sent_contracts(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT contract_address FROM alerts")
            rows = cursor.fetchall()
            for row in rows:
                self.sent_contracts.add(row[0])
        logger.info(f"Loaded {len(self.sent_contracts)} contracts from storage.")

    def save_contract(self, contract_address: str):
        self.sent_contracts.add(contract_address)
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO alerts (contract_address, timestamp) VALUES (?, ?)",
                    (contract_address, int(time.time()))
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed writing database commit: {e}")

    async def send_telegram_alert(self, client: httpx.AsyncClient, message: str):
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        try:
            resp = await client.post(url, json=payload, timeout=10.0)
            if resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
                await asyncio.sleep(retry_after)
                await client.post(url, json=payload, timeout=10.0)
        except Exception as e:
            logger.error(f"Telegram API call error: {e}")

    async def fetch_detailed_pair_data(self, client: httpx.AsyncClient, token_address: str):
        url = f"{DEX_TOKEN_BASE_URL}/{token_address}"
        try:
            response = await client.get(url, timeout=10.0)
            if response.status_code == 200:
                pairs = response.json()
                if isinstance(pairs, dict) and "pairs" in pairs:
                    return pairs["pairs"]
                return pairs
            return None
        except Exception as e:
            return None

    async def monitor_pipeline(self):
        logger.info("Starting Solana Tracking Engine background loop...")
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        
        async with httpx.AsyncClient(limits=limits) as client:
            while True:
                try:
                    response = await client.get(DEXSCREENER_PROFILES_URL, timeout=10.0)
                    if response.status_code == 429:
                        await asyncio.sleep(30)
                        continue
                    if response.status_code != 200:
                        await asyncio.sleep(CHECK_INTERVAL)
                        continue

                    profiles = response.json()
                    if not isinstance(profiles, list):
                        await asyncio.sleep(CHECK_INTERVAL)
                        continue

                    for profile in profiles:
                        if profile.get("chainId") != "solana":
                            continue
                            
                        token_address = profile.get("tokenAddress")
                        if not token_address or token_address in self.sent_contracts:
                            continue

                        links = profile.get("links", [])
                        telegram_link = None
                        has_website = False

                        for link in links:
                            link_type = link.get("type", "").lower()
                            link_url = link.get("url", "")
                            if "telegram" in link_type or "t.me" in link_url:
                                telegram_link = link_url
                            if "website" in link_type or link_type == "website":
                                has_website = True

                        if not telegram_link or has_website:
                            continue

                        pairs_data = await self.fetch_detailed_pair_data(client, token_address)
                        if not pairs_data:
                            continue

                        sol_pairs = [p for p in pairs_data if p.get("chainId") == "solana"]
                        if not sol_pairs:
                            continue
                        
                        primary_pair = max(sol_pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0) if x.get("liquidity") else 0)

                        pair_created_at = primary_pair.get("pairCreatedAt", 0)
                        if not pair_created_at:
                            continue
                            
                        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                        age_minutes = (now_ms - pair_created_at) / (1000 * 60)

                        if age_minutes > 60 or age_minutes < -5:
                            continue

                        liquidity_usd = primary_pair.get("liquidity", {}).get("usd", 0)
                        volume_h1 = primary_pair.get("volume", {}).get("h1", 0)
                        
                        if liquidity_usd < 1000 or volume_h1 < 500:
                            continue

                        token_name = primary_pair.get("baseToken", {}).get("name", "Unknown")
                        symbol = primary_pair.get("baseToken", {}).get("symbol", "UNKNOWN")
                        pair_address = primary_pair.get("pairAddress", "N/A")
                        market_cap = primary_pair.get("marketCap", 0)
                        price_usd = primary_pair.get("priceUsd", "0.0")
                        buys = primary_pair.get("txns", {}).get("h1", {}).get("buys", 0)
                        sells = primary_pair.get("txns", {}).get("h1", {}).get("sells", 0)
                        dex_url = primary_pair.get("url", f"https://dexscreener.com/solana/{pair_address}")

                        age_str = f"{int(age_minutes)}m ago" if age_minutes >= 1 else "Just Now (<1m)"

                        alert_msg = (
                            f"🚀 <b>New Solana Token Detected</b>\n\n"
                            f"🪙 <b>Name:</b> {token_name}\n"
                            f"📌 <b>Symbol:</b> ${symbol}\n"
                            f"💧 <b>Liquidity:</b> ${liquidity_usd:,.2f}\n"
                            f"📊 <b>Market Cap:</b> ${market_cap:,.2f}\n"
                            f"📈 <b>Volume (1h):</b> ${volume_h1:,.2f}\n"
                            f"🕒 <b>Age:</b> {age_str}\n"
                            f"📬 <b>Telegram:</b> {telegram_link}\n"
                            f"📊 <b>Txns (1h):</b> {buys} Buys / {sells} Sells\n"
                            f"💵 <b>Price:</b> ${price_usd}\n"
                            f"📃 <b>Contract:</b> <code>{token_address}</code>\n"
                            f"🔗 <a href='{dex_url}'>DexScreener Link</a>"
                        )

                        self.save_contract(token_address)
                        await self.send_telegram_alert(client, alert_msg)

                except Exception as ce:
                    logger.error(f"Loop error: {ce}")
                
                await asyncio.sleep(CHECK_INTERVAL)

# --- WEB SERVER LIFECYCLE HOOK ---
@app.on_event("startup")
async def startup_event():
    # Spawns our async tracking pipeline safely in the background
    bot = TokenTrackerBot()
    asyncio.create_task(bot.monitor_pipeline())
