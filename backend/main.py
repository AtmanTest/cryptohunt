#!/usr/bin/env python3
"""CryptoHunt Backend — FastAPI sur Render."""
import asyncio, json, time, math, random
from datetime import datetime, timezone
from collections import deque

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="CryptoHunt API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_cache = {"data": None, "meta": {}, "ts": 0, "raw": None}
LOCK = asyncio.Lock()
WEBSOCKETS: set[WebSocket] = set()

# ── Helpers ───────

def rsi(prices: list[float], period: int = 14) -> float | None:
    if len(prices) < period + 1: return None
    gains = losses = 0.0
    for i in range(-period, 0):
        diff = prices[i] - prices[i-1]
        if diff > 0: gains += diff
        else: losses -= diff
    avg_gain, avg_loss = gains/period, losses/period
    if avg_loss == 0: return 100.0
    return round(100.0 - 100.0/(1.0 + avg_gain/avg_loss), 1)

def sma(prices: list[float], period: int) -> float | None:
    if len(prices) < period: return None
    return round(sum(prices[-period:])/period, 4)

def trend_score(coin: dict, history: list) -> dict:
    prices = [h["price"] for h in history] if history else []
    score = 50
    result = {"rsi_14": None, "sma_50": None, "sma_200": None, "momentum_24h": None, "trend_score": 50}
    if len(prices) >= 2:
        mom = (prices[-1]/prices[-2]-1)*100
        result["momentum_24h"] = round(mom, 2)
        if mom > 5: score += 15
        elif mom > 2: score += 8
        elif mom < -5: score -= 15
        elif mom < -2: score -= 8
    r = rsi(prices)
    result["rsi_14"] = r
    if r is not None:
        if r > 70: score -= 10
        elif r < 30: score += 10
        elif 40 <= r <= 60: score += 5
    s50 = sma(prices, 50)
    result["sma_50"] = s50
    s200 = sma(prices, 200)
    result["sma_200"] = s200
    if s50 and s200 and s50 > s200: score += 10
    elif s50 and s200 and s50 < s200: score -= 10
    if history:
        avg_vol = sum(h.get("volume",0) or 0 for h in history)/len(history)
        cur_vol = coin.get("volumeUsd",0) or 0
        result["volume_spike"] = avg_vol > 0 and cur_vol > avg_vol*2
    result["trend_score"] = max(0, min(100, score))
    return result

# ── Data fetching ───
# Use CoinGecko markets endpoint (200 coins per page)
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

async def fetch_coingecko_markets(client: httpx.AsyncClient, page: int = 1, per_page: int = 250) -> list[dict]:
    """Get top coins by market cap from CoinGecko."""
    try:
        url = f"{COINGECKO_BASE}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "24h",
        }
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        resp = await client.get(url, params=params, headers=headers, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            print(f"[CRYPTOHUNT] CoinGecko markets: {len(data)} coins (page {page}), status {resp.status_code}")
            return data
        else:
            print(f"[CRYPTOHUNT] CoinGecko markets status: {resp.status_code}")
            return []
    except Exception as e:
        print(f"[CRYPTOHUNT] CoinGecko markets error: {e}")
        return []

async def fetch_coinpaprika(client: httpx.AsyncClient) -> list[dict]:
    """Fallback: CoinPaprika ticker endpoint."""
    try:
        resp = await client.get("https://api.coinpaprika.com/v1/tickers?limit=250", timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            print(f"[CRYPTOHUNT] CoinPaprika: {len(data)} coins")
            return data
        print(f"[CRYPTOHUNT] CoinPaprika status: {resp.status_code}")
        return []
    except Exception as e:
        print(f"[CRYPTOHUNT] CoinPaprika error: {e}")
        return []

async def fetch_global_meta(client: httpx.AsyncClient) -> dict:
    """Global market data."""
    meta = {"btc_dominance": None, "eth_dominance": None, "total_mcap": None, "fear_greed": None}
    # CoinGecko global
    try:
        resp = await client.get(f"{COINGECKO_BASE}/global", timeout=10,
                                headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            d = resp.json().get("data", {})
            meta["total_mcap"] = d.get("total_market_cap", {}).get("usd")
            mcap_pct = d.get("market_cap_percentage", {})
            meta["btc_dominance"] = round(mcap_pct.get("btc", 0), 1)
            meta["eth_dominance"] = round(mcap_pct.get("eth", 0), 1)
    except: pass
    # Fear & Greed
    try:
        resp = await client.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        if resp.status_code == 200:
            d = resp.json().get("data", [])
            if d:
                meta["fear_greed"] = {"value": int(d[0].get("value", 50)), "classification": d[0].get("value_classification", "Neutral")}
    except: pass
    return meta

# ── Refresh ───

async def refresh_cache():
    async with httpx.AsyncClient() as client:
        try:
            # Primary: CoinGecko markets
            cg_data = await fetch_coingecko_markets(client, page=1, per_page=250)
            # If we got <250, try page 2 for the rest
            if len(cg_data) >= 200:
                page2 = await fetch_coingecko_markets(client, page=2, per_page=100)
                cg_data.extend(page2)

            assets = cg_data
            source = "coingecko"

            # Fallback: CoinPaprika if CG returned nothing
            if not assets:
                pap = await fetch_coinpaprika(client)
                if pap:
                    assets = pap
                    source = "coinpaprika"

            if not assets:
                print(f"[CRYPTOHUNT] No data from any source, keeping old cache")
                return  # keep old cache

            meta = await fetch_global_meta(client)

            # ── Normalize assets to consistent format ──
            processed = []
            for a in assets:
                if source == "coingecko":
                    price = float(a.get("current_price", 0) or 0)
                    mcap = float(a.get("market_cap", 0) or 0)
                    vol = float(a.get("total_volume", 0) or 0)
                    supply = float(a.get("circulating_supply", 0) or 0)
                    max_s = a.get("max_supply")
                    max_s = float(max_s) if max_s else None
                    chg = a.get("price_change_percentage_24h")
                    chg = round(float(chg), 2) if chg else None
                    name = a.get("name", "")
                    symbol = a.get("symbol", "").upper()
                    coin_id = a.get("id", "")
                else:  # coinpaprika
                    q = a.get("quotes", {}).get("USD", {})
                    price = float(q.get("price", 0) or 0)
                    mcap = float(q.get("market_cap", 0) or 0)
                    vol = float(q.get("volume_24h", 0) or 0)
                    supply = float(a.get("circulating_supply", 0) or 0)
                    max_s = a.get("max_supply")
                    max_s = float(max_s) if max_s else None
                    chg = q.get("percent_change_24h")
                    chg = round(float(chg), 2) if chg else None
                    name = a.get("name", "")
                    symbol = a.get("symbol", "").upper()
                    coin_id = a.get("id", "")

                vol_mcap = round(vol/mcap, 4) if mcap > 0 else 0

                processed.append({
                    "id": coin_id, "rank": None, "name": name, "symbol": symbol,
                    "price": round(price, 8) if price < 0.01 else round(price, 4) if price < 1 else round(price, 2),
                    "price_raw": price, "mcap": int(mcap), "volume24h": int(vol),
                    "supply": int(supply), "max_supply": max_s, "change24h": chg, "vwap24h": None,
                    "vol_mcap_ratio": vol_mcap, "rsi_14": None, "sma_50": None, "sma_200": None,
                    "momentum_24h": chg, "trend_score": 50, "volume_spike": False,
                })

            # Remove BTC if it's first
            if processed and processed[0]["symbol"] == "BTC":
                meta["btc"] = processed.pop(0)

            top_300 = processed[:300]
            now = int(time.time())
            async with LOCK:
                _cache["data"] = top_300
                _cache["meta"] = meta
                _cache["ts"] = now
                _cache["raw"] = processed

            print(f"[CRYPTOHUNT] Cache rafraîchi : {len(top_300)} coins via {source}, {datetime.now().isoformat()}")

            payload = json.dumps({"type": "update", "coins": top_300, "meta": meta, "ts": now})
            dead = set()
            for ws in WEBSOCKETS:
                try: await ws.send_text(payload)
                except: dead.add(ws)
            WEBSOCKETS -= dead
        except Exception as e:
            print(f"[CRYPTOHUNT] refresh_cache error: {e}")
            import traceback; traceback.print_exc()

# ── Startup ───

@app.on_event("startup")
async def startup():
    print("[CRYPTOHUNT] Server starting, first refresh...")
    await refresh_cache()
    asyncio.create_task(_periodic_refresh())

async def _periodic_refresh():
    while True:
        await asyncio.sleep(90)
        await refresh_cache()

# ── Routes ───

@app.get("/")
async def root():
    return {"name": "CryptoHunt API", "version": "2.0", "endpoints": ["/api/top300", "/api/snapshot", "/api/health", "/api/debug", "/ws"]}

@app.get("/api/health")
async def health():
    async with LOCK:
        age = int(time.time()) - _cache["ts"]
        return {"status": "ok", "coins_cached": len(_cache["data"] or []), "cache_age_seconds": age}

@app.get("/api/debug")
async def debug():
    results = {}
    async with httpx.AsyncClient() as client:
        for name, url in [
            ("coingecko_markets", f"{COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=1&page=1"),
            ("coingecko_global", f"{COINGECKO_BASE}/global"),
            ("coinpaprika", "https://api.coinpaprika.com/v1/tickers?limit=1"),
            ("fng", "https://api.alternative.me/fng/?limit=1"),
        ]:
            try:
                r = await client.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                results[name] = {"status": r.status_code, "ok": r.status_code == 200}
            except Exception as e:
                results[name] = {"error": str(e)[:80]}
    return results

@app.get("/api/test-apis")
async def test_apis():
    """Test toutes les APIs de la liste pour voir lesquelles répondent sur Render."""
    apis = [
        ("coingecko_pro", "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=1"),
        ("coingecko_simple", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"),
        ("coingecko_global", "https://api.coingecko.com/api/v3/global"),
        ("coincap", "https://api.coincap.io/v2/assets?limit=1"),
        ("coinpaprika", "https://api.coinpaprika.com/v1/tickers?limit=1"),
        ("coinmarketcap_pro", "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=1"),
        ("binance_ticker", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"),
        ("binance_24hr", "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT"),
        ("coindesk_asset", "https://data-api.coindesk.com/asset/v1/top/list?limit=1"),
        ("coincodex", "https://coincodex.com/api/coincodex/get_coin_summary/"),
        ("dexscreener_pairs", "https://api.dexscreener.com/latest/v2/pairs/ethereum/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"),
        ("defillama_tvl", "https://api.llama.fi/tvl/uniswap"),
        ("defillama_protocols", "https://api.llama.fi/protocols"),
        ("1inch_health", "https://api.1inch.dev/swap/v6.0/1/healthcheck"),
        ("bitquery", "https://graphql.bitquery.io/ide"),
        ("mnemonic", "https://api.mnemonic.fi/ping"),
        ("dune", "https://api.dune.com/api/v1/health"),
        ("alternative_fng", "https://api.alternative.me/fng/?limit=1"),
    ]
    results = {}
    async with httpx.AsyncClient(timeout=15.0) as client:
        for name, url in apis:
            try:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                results[name] = {"status": r.status_code, "ok": r.status_code < 500}
                if r.status_code == 429:
                    results[name]["note"] = "rate_limited"
                elif r.status_code == 403:
                    results[name]["note"] = "forbidden_no_key"
                elif 200 <= r.status_code < 300:
                    body = r.text[:200]
                    results[name]["preview"] = body[:100]
            except httpx.TimeoutException:
                results[name] = {"error": "timeout"}
            except Exception as e:
                results[name] = {"error": str(e)[:80]}
    return results

@app.get("/api/top300")
async def get_top300():
    async with LOCK:
        if not _cache["data"]:
            return JSONResponse({"error": "Cache pas encore prêt"}, status_code=503)
        return {"coins": _cache["data"], "meta": _cache["meta"], "ts": _cache["ts"], "count": len(_cache["data"])}

@app.get("/api/snapshot")
async def get_snapshot():
    async with LOCK:
        return {"ts": _cache["ts"], "coins": _cache["data"], "meta": _cache["meta"]}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    WEBSOCKETS.add(ws)
    async with LOCK:
        if _cache["data"]:
            payload = json.dumps({"type": "update", "coins": _cache["data"], "meta": _cache["meta"], "ts": _cache["ts"]})
            await ws.send_text(payload)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        WEBSOCKETS.discard(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
