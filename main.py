import asyncio
import json
import logging
import os
import signal
import sys
import httpx
from fastapi import FastAPI
from websockets import connect

# ----------------------------------------------------
# ENVIRONMENT VARIABLES & CONFIGURATION
# ----------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8317307510:AAHqbY63j5vlt70WaaFZGe2We40E-E_fMyM")
CHAT_ID = os.getenv("CHAT_ID", "6011460052")
SOLANA_RPC_WS = os.getenv("SOLANA_RPC_WS")
SOLANA_RPC_HTTP = os.getenv("SOLANA_RPC_HTTP")

# Target Dex Infrastructure Programs
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_AMM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CLMM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"

MIN_LIQUIDITY_USD = 0.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# FastAPI app instantiated to satisfy Koyeb Health Routing Layer
app = FastAPI()

@app.get("/")
async def health_check():
    return {"status": "healthy", "pipeline": "Solana Live Mint Radar"}


class LiveMintRadar:
    def __init__(self):
        self.http_client = None
        self.processing_queue = asyncio.Queue()
        self.alert_queue = asyncio.Queue()
        self.is_running = True

    async def init_client(self):
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=30)
        self.http_client = httpx.AsyncClient(limits=limits, timeout=10.0)

    async def close_client(self):
        if self.http_client:
            await self.http_client.aclose()

    # ----------------------------------------------------
    # CORE TASKS: PIPELINE AGGREGATOR
    # ----------------------------------------------------
    async def ws_listener(self):
        """Maintains low-latency WebSocket logs subscription loop"""
        backoff = 1.0
        while self.is_running:
            try:
                logger.info("Connecting to Solana WebSocket Stream...")
                async with connect(SOLANA_RPC_WS) as ws:
                    backoff = 1.0  # Reset on successful link
                    
                    # Target all structural logs mentioning our target programs
                    payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [PUMP_FUN_PROGRAM, RAYDIUM_AMM, RAYDIUM_CLMM]},
                            {"commitment": "processed"}
                        ]
                    }
                    await ws.send(json.dumps(payload))
                    logger.info("Subscription active across target liquidity protocols.")

                    async for message in ws:
                        if not self.is_running:
                            break
                        data = json.loads(message)
                        result = data.get("params", {}).get("result", {})
                        if not result:
                            continue

                        logs = result.get("value", {}).get("logs", [])
                        signature = result.get("value", {}).get("signature")

                        # Fast pattern check inside transaction strings
                        is_pump = any("Instruction: Create" in log for log in logs)
                        is_raydium = any("initialize2: InitializeInstruction" in log or "createPool" in log for log in logs)

                        if is_pump or is_raydium:
                            logger.info(f"Detected initialization signature footprint: {signature}")
                            await self.processing_queue.put(signature)

            except Exception as e:
                logger.error(f"WebSocket execution exception: {e}. Reconnecting...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def token_fetcher(self):
        """Pulls signatures out of queue to parse raw text and fetch coin metrics"""
        while self.is_running:
            try:
                signature = await self.processing_queue.get()
                mint_address = await self.extract_mint_via_http(signature)
                
                if mint_address:
                    # Execute API profiling concurrently
                    asyncio.create_task(self.profile_and_route(mint_address))
                
                self.processing_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reading processing stack element: {e}")

    async def alert_sender(self):
        """Asynchronously dispatches formatted cards directly to Telegram endpoint"""
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        while self.is_running:
            try:
                payload_msg = await self.alert_queue.get()
                response = await self.http_client.post(url, json=payload_msg)
                
                if response.status_code == 429:
                    retry_after = response.json().get("parameters", {}).get("retry_after", 5)
                    logger.warning(f"Telegram Rate limit hit. Backing off for {retry_after}s.")
                    await asyncio.sleep(retry_after)
                    await self.alert_queue.put(payload_msg)  # Re-enqueue
                
                self.alert_queue.task_done()
                await asyncio.sleep(0.1)  # Guard rails
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Alert output dispatcher caught failure: {e}")

    # ----------------------------------------------------
    # EXTRACTION & METRIC VALIDATION UTILITIES
    # ----------------------------------------------------
    async def extract_mint_via_http(self, signature: str) -> str:
        """Fetches block contents to manually locate custom token mint components"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
        }
        try:
            response = await self.http_client.post(SOLANA_RPC_HTTP, json=payload)
            if response.status_code != 200:
                return None
            
            tx_data = response.json().get("result", {})
            if not tx_data:
                return None

            meta = tx_data.get("meta", {})
            post_balances = meta.get("postTokenBalances", [])
            
            for balance in post_balances:
                mint = balance.get("mint")
                # Filter out the standard native wrap ledger
                if mint and mint != "So11111111111111111111111111111111111111112":
                    return mint
        except Exception as e:
            logger.error(f"Failed parsing block transaction signature data {signature}: {e}")
        return None

    async def profile_and_route(self, mint: str):
        """Validates depth checks and structures the layout card mapping template"""
        # Concurrent evaluation data retrieval
        dex_task = self.http_client.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
        rug_task = self.http_client.get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report")

        try:
            dex_resp, rug_resp = await asyncio.gather(dex_task, rug_task, return_exceptions=True)
            
            # Extract DEX data
            pairs = []
            if not isinstance(dex_resp, Exception) and dex_resp.status_code == 200:
                pairs = dex_resp.json().get("pairs", []) or []

            # If DexScreener hasn't indexed it yet, check liquidity parameters safely
            liquidity = 0.0
            mcap = 0.0
            price = "0.00"
            name = "Unknown"
            symbol = "UNKNOWN"

            if pairs:
                primary_pair = max(pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0) if x.get("liquidity") else 0)
                liquidity = float(primary_pair.get("liquidity", {}).get("usd", 0) or 0)
                mcap = float(primary_pair.get("marketCap", 0) or 0)
                price = primary_pair.get("priceUsd", "0.00")
                name = primary_pair.get("baseToken", {}).get("name", "Unknown")
                symbol = primary_pair.get("baseToken", {}).get("symbol", "UNKNOWN")

            # Apply constraints logic filter out noise
            if pairs and liquidity < MIN_LIQUIDITY_USD:
                return

            # Extract Rugcheck metric payload
            rug_score = "Unknown"
            if not isinstance(rug_resp, Exception) and rug_resp.status_code == 200:
                score = rug_resp.json().get("score")
                rug_score = str(score) if score is not None else "Good"

            # Formulate Markdown / HTML Presentation Card layout
            alert_text = (
                f"🚨 <b>NEW SOLANA MEME TOKEN SPOTTED</b> 🚨\n\n"
                f"🪙 <b>Token:</b> {name} ({symbol})\n"
                f"📃 <b>Mint/CA:</b>\n<code>{mint}</code>\n\n"
                f"📊 <b>Market Cap:</b> ${mcap:,.2f}\n"
                f"💧 <b>Liquidity:</b> ${liquidity:,.2f}\n"
                f"💵 <b>Price:</b> ${price}\n"
                f"🛡️ <b>RugCheck Score:</b> <code>{rug_score}</code>\n\n"
                f"🔗 <a href='https://dexscreener.com/solana/{mint}'>Dexscreener</a> | "
                f"<a href='https://birdeye.so/token/{mint}?chain=solana'>Birdeye</a> | "
                f"<a href='https://solscan.io/token/{mint}'>Solscan</a>"
            )

            payload_msg = {
                "chat_id": CHAT_ID,
                "text": alert_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            await self.alert_queue.put(payload_msg)

        except Exception as e:
            logger.error(f"Error building analytics profile for mint {mint}: {e}")

    async def shutdown(self):
        logger.info("Graceful execution shutdown signal triggered...")
        self.is_running = False
        await self.close_client()


radar = LiveMintRadar()

@app.on_event("startup")
async def startup_event():
    await radar.init_client()
    asyncio.create_task(radar.ws_listener())
    asyncio.create_task(radar.token_fetcher())
    asyncio.create_task(radar.alert_sender())

@app.on_event("shutdown")
async def shutdown_event():
    await radar.shutdown()
                                                              
