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
LOG_BUFFER: list[str] = []
LOCK = asyncio.Lock()
WEBSOCKETS: set[WebSocket] = set()

# ── Helpers ───────

def log(msg: str):
    """Log + buffer for /api/logs endpoint."""
    print(msg)
    LOG_BUFFER.append(msg)
    if len(LOG_BUFFER) > 100:
        LOG_BUFFER[:50] = []

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
COINDESK_BASE = "https://data-api.coindesk.com/asset/v1"

async def fetch_coindesk_page(client: httpx.AsyncClient, page: int = 1) -> list[dict]:
    """Primary source: CoinDesk asset list, sorted by market cap desc (100 per page)."""
    try:
        url = f"{COINDESK_BASE}/top/list"
        params = {"limit": 100, "page": page, "sort_by": "CIRCULATING_MKT_CAP_USD", "sort_dir": "DESC"}
        resp = await client.get(url, params=params, timeout=20,
                                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        if resp.status_code == 200:
            data = resp.json().get("Data", {}).get("LIST", [])
            log(f"[CRYPTOHUNT] CoinDesk page {page}: {len(data)} assets")
            return data
        log(f"[CRYPTOHUNT] CoinDesk page {page}: HTTP {resp.status_code}")
        return []
    except Exception as e:
        log(f"[CRYPTOHUNT] CoinDesk page {page} error: {e}")
        return []

async def fetch_coinpaprika(client: httpx.AsyncClient) -> list[dict]:
    """Fallback: CoinPaprika ticker endpoint."""
    try:
        resp = await client.get("https://api.coinpaprika.com/v1/tickers?limit=250", timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            log(f"[CRYPTOHUNT] CoinPaprika: {len(data)} coins")
            return data
        log(f"[CRYPTOHUNT] CoinPaprika status: {resp.status_code}")
        return []
    except Exception as e:
        log(f"[CRYPTOHUNT] CoinPaprika error: {e}")
        return []

async def fetch_global_meta(client: httpx.AsyncClient) -> dict:
    """Global market data from Fear & Greed + DeFiLlama."""
    meta = {"btc_dominance": None, "eth_dominance": None, "total_mcap": None, "fear_greed": None, "defi_tvl": None}
    # Fear & Greed
    try:
        resp = await client.get("https://api.alternative.me/fng/?limit=1", timeout=5,
                                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        if resp.status_code == 200:
            d = resp.json().get("data", [])
            if d:
                meta["fear_greed"] = {"value": int(d[0].get("value", 50)), "classification": d[0].get("value_classification", "Neutral")}
    except Exception as e:
        log(f"[CRYPTOHUNT] F&G error: {e}")
    # DeFiLlama TVL - using protocols (lightweight enough)
    try:
        resp = await client.get("https://api.llama.fi/protocols", timeout=15,
                                headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            total = 0
            count = 0
            for p in data:
                if isinstance(p, dict):
                    tvl = p.get("tvl", 0) or 0
                    total += tvl
                    count += 1
            if total > 0:
                meta["defi_tvl"] = {"value": total, "chains": count, "source": "defillama"}
                log(f"[CRYPTOHUNT] DeFi TVL: ${total/1e9:.1f}B from {count} protocols")
    except Exception as e:
        log(f"[CRYPTOHUNT] DeFiLlama error: {e}")
    # Dominance from CoinDesk (already fetched, but we calculate from top 100)
    try:
        cd_url = f"{COINDESK_BASE}/top/list"
        cd_params = {"limit": 100, "page": 1, "sort_by": "CIRCULATING_MKT_CAP_USD", "sort_dir": "DESC"}
        resp = await client.get(cd_url, params=cd_params, timeout=10,
                                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        if resp.status_code == 200:
            assets = resp.json().get("Data", {}).get("LIST", [])
            total = sum(float(a.get("CIRCULATING_MKT_CAP_USD", 0) or 0) for a in assets)
            if total > 0:
                for a in assets:
                    sym = a.get("SYMBOL", "")
                    cap = float(a.get("CIRCULATING_MKT_CAP_USD", 0) or 0)
                    if sym == "BTC": meta["btc_dominance"] = round(cap / total * 100, 1)
                    if sym == "ETH": meta["eth_dominance"] = round(cap / total * 100, 1)
                meta["total_mcap"] = int(total)
                log(f"[CRYPTOHUNT] Dominance: BTC {meta['btc_dominance']}% / ETH {meta['eth_dominance']}%, total ${total/1e9:.1f}B")
    except Exception as e:
        log(f"[CRYPTOHUNT] Dominance error: {e}")
    return meta

def normalize_coindesk(a: dict) -> dict:
    """Normalize a CoinDesk asset to our standard format."""
    price = float(a.get("PRICE_USD", 0) or 0)
    mcap = float(a.get("CIRCULATING_MKT_CAP_USD", 0) or 0)
    vol = float(a.get("SPOT_MOVING_24_HOUR_QUOTE_VOLUME_USD", 0) or 0)
    supply = float(a.get("SUPPLY_CIRCULATING", 0) or 0)
    max_s = a.get("SUPPLY_MAX")
    max_s = float(max_s) if max_s else None
    chg = a.get("SPOT_MOVING_24_HOUR_CHANGE_PERCENTAGE_USD")
    chg = round(float(chg), 2) if chg else None
    symbol = a.get("SYMBOL", "").upper()
    name = a.get("NAME", "")
    coin_id = f"cd_{symbol}"
    vol_mcap = round(vol/mcap, 4) if mcap > 0 else 0

    return {
        "id": coin_id, "rank": rank, "name": name, "symbol": symbol,
        "price": round(price, 8) if price < 0.01 else round(price, 4) if price < 1 else round(price, 2),
        "price_raw": price, "mcap": int(mcap), "volume24h": int(vol),
        "supply": int(supply), "max_supply": max_s, "change24h": chg, "vwap24h": None,
        "vol_mcap_ratio": vol_mcap, "rsi_14": None, "sma_50": None, "sma_200": None,
        "momentum_24h": chg, "trend_score": 50, "volume_spike": False,
    }

def normalize_coinpaprika(a: dict) -> dict:
    """Normalize a CoinPaprika ticker to our format."""
    q = a.get("quotes", {}).get("USD", {})
    price = float(q.get("price", 0) or 0)
    mcap = float(q.get("market_cap", 0) or 0)
    vol = float(q.get("volume_24h", 0) or 0)
    supply = float(a.get("circulating_supply", 0) or 0)
    max_s = a.get("max_supply")
    max_s = float(max_s) if max_s else None
    chg = q.get("percent_change_24h")
    chg = round(float(chg), 2) if chg else None
    symbol = a.get("symbol", "").upper()
    name = a.get("name", "")
    coin_id = a.get("id", "")
    vol_mcap = round(vol/mcap, 4) if mcap > 0 else 0

    return {
        "id": coin_id, "rank": a.get("rank"), "name": name, "symbol": symbol,
        "price": round(price, 8) if price < 0.01 else round(price, 4) if price < 1 else round(price, 2),
        "price_raw": price, "mcap": int(mcap), "volume24h": int(vol),
        "supply": int(supply), "max_supply": max_s, "change24h": chg, "vwap24h": None,
        "vol_mcap_ratio": vol_mcap, "rsi_14": None, "sma_50": None, "sma_200": None,
        "momentum_24h": chg, "trend_score": 50, "volume_spike": False,
    }

# ── Refresh ───

async def refresh_cache():
    log("[CRYPTOHUNT] refresh_cache START")
    async with httpx.AsyncClient() as client:
        try:
            # Tier 1 — CoinDesk: 3 pages of 100 = top 300 by market cap
            page1 = await fetch_coindesk_page(client, 1)
            log(f"[CRYPTOHUNT] page1: {len(page1)} items")
            page2 = await fetch_coindesk_page(client, 2)
            log(f"[CRYPTOHUNT] page2: {len(page2)} items")
            page3 = await fetch_coindesk_page(client, 3)
            log(f"[CRYPTOHUNT] page3: {len(page3)} items")
            cd_assets = page1 + page2 + page3
            log(f"[CRYPTOHUNT] cd_assets total: {len(cd_assets)}")

            assets = []
            source = None
            if cd_assets:
                try:
                    raw = [normalize_coindesk(a) for a in cd_assets]
                    log(f"[CRYPTOHUNT] normalize_coindesk OK: {len(raw)} items")
                    assets = raw
                    source = "coindesk"
                except Exception as norm_e:
                    log(f"[CRYPTOHUNT] normalize_coindesk failed: {norm_e}")
                    import traceback; traceback.print_exc()
            else:
                log("[CRYPTOHUNT] CoinDesk empty, trying CoinPaprika fallback")
                pap = await fetch_coinpaprika(client)
                if pap:
                    total_mcap = sum(
                        float(a.get("quotes", {}).get("USD", {}).get("market_cap", 0) or 0)
                        for a in pap
                    )
                    log(f"[CRYPTOHUNT] Paprika fallback: {len(pap)} coins, total mcap {total_mcap}")
                    assets = [normalize_coinpaprika(a) for a in pap]
                    source = "coinpaprika"

            log(f"[CRYPTOHUNT] assets after source selection: {len(assets)} from {source}")
            if not assets:
                log("[CRYPTOHUNT] No data from any source, keeping old cache")
                return

            meta = await fetch_global_meta(client)
            log(f"[CRYPTOHUNT] meta: fear_greed={meta.get('fear_greed')}, defi_tvl={bool(meta.get('defi_tvl'))}, btc_dom={meta.get('btc_dominance')}")

            if assets and assets[0]["symbol"] == "BTC":
                meta["btc"] = assets.pop(0)
                log(f"[CRYPTOHUNT] BTC extracted: ${meta['btc']['price_raw']}")

            top_300 = assets[:300]
            for i, c in enumerate(top_300):
                c["rank"] = i + 1
            now = int(time.time())
            async with LOCK:
                _cache["data"] = top_300
                _cache["meta"] = meta
                _cache["ts"] = now
                _cache["raw"] = assets
                log(f"[CRYPTOHUNT] CACHE SET: {len(top_300)} coins, ts={now}")

            log(f"[CRYPTOHUNT] Cache rafraîchi : {len(top_300)} coins via {source}, {datetime.now().isoformat()}")

            payload = json.dumps({"type": "update", "coins": top_300, "meta": meta, "ts": now})
            dead = set()
            for ws in WEBSOCKETS:
                try: await ws.send_text(payload)
                except: dead.add(ws)
            WEBSOCKETS -= dead
        except Exception as e:
            log(f"[CRYPTOHUNT] refresh_cache error: {e}")
            import traceback; traceback.print_exc()

# ── Startup ───

@app.on_event("startup")
async def startup():
    log("[CRYPTOHUNT] Server starting, first refresh...")
    await refresh_cache()
    asyncio.create_task(_periodic_refresh())

async def _periodic_refresh():
    while True:
        await asyncio.sleep(90)
        await refresh_cache()

# ── Routes ───

@app.get("/")
async def root():
    return {"name": "CryptoHunt API", "version": "v7", "endpoints": ["/api/top300", "/api/snapshot", "/api/health", "/api/debug", "/api/test-apis", "/ws"]}

@app.get("/api/health")
async def health():
    async with LOCK:
        age = int(time.time()) - _cache["ts"]
        return {"status": "ok", "coins_cached": len(_cache["data"] or []), "cache_age_seconds": age}

@app.get("/api/debug")
async def debug():
    results = {}
    async with httpx.AsyncClient() as client:
        for name, url, to in [
            ("coindesk_p1", "https://data-api.coindesk.com/asset/v1/top/list?limit=100&page=1&sort_by=CIRCULATING_MKT_CAP_USD&sort_dir=DESC", 10),
            ("coinpaprika", "https://api.coinpaprika.com/v1/tickers?limit=250", 20),
            ("fng", "https://api.alternative.me/fng/?limit=1", 5),
            ("defillama_protocols", "https://api.llama.fi/protocols", 15),
        ]:
            try:
                r = await client.get(url, timeout=to, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                results[name] = {"status": r.status_code, "ok": r.status_code == 200, "bytes": len(r.text)}
            except Exception as e:
                results[name] = {"error": str(e)[:80]}
    return results

@app.get("/api/refresh")
async def manual_refresh():
    """Force un rafraîchissement manuel du cache."""
    try:
        await refresh_cache()
        async with LOCK:
            return {"status": "ok", "coins": len(_cache["data"] or [])}
    except Exception as e:
        import traceback
        return {"status": "error", "error": str(e), "traceback": traceback.format_exc()[:500]}

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

@app.get("/api/logs")
async def get_logs():
    """Dernières lignes de log du serveur."""
    return {"logs": LOG_BUFFER[-80:]}

@app.get("/api/diag")
async def diag():
    """Diagnostics détaillés pour debugger le refresh."""
    import traceback
    result = {"cache": {}}
    async with LOCK:
        result["cache"]["age"] = int(time.time()) - _cache["ts"]
        result["cache"]["coins"] = len(_cache["data"] or [])
        result["cache"]["meta_keys"] = list((_cache.get("meta") or {}).keys())

    async with httpx.AsyncClient() as client:
        for page in [1, 2, 3]:
            try:
                url = "https://data-api.coindesk.com/asset/v1/top/list"
                params = {"limit": 100, "page": page, "sort_by": "CIRCULATING_MKT_CAP_USD", "sort_dir": "DESC"}
                r = await client.get(url, params=params, timeout=15,
                                     headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                if r.status_code == 200:
                    data = r.json().get("Data", {}).get("LIST", [])
                    result[f"coindesk_p{page}"] = {"status": 200, "count": len(data)}
                    if data:
                        result[f"coindesk_p{page}"]["first_sym"] = data[0].get("SYMBOL", "")
                        result[f"coindesk_p{page}"]["first_price"] = data[0].get("PRICE_USD")
                else:
                    result[f"coindesk_p{page}"] = {"status": r.status_code, "body": r.text[:100]}
            except Exception as e:
                result[f"coindesk_p{page}"] = {"error": str(e)[:200]}

        try:
            r = await client.get("https://api.coinpaprika.com/v1/tickers?limit=5", timeout=15)
            result["paprika"] = {"status": r.status_code}
        except Exception as e:
            result["paprika"] = {"error": str(e)[:100]}

    return result
