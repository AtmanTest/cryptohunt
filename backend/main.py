#!/usr/bin/env python3
"""CryptoHunt Backend — FastAPI sur Render, agrège CoinCap + CoinGecko."""

import asyncio, json, time, math
from datetime import datetime, timezone
from collections import deque

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="CryptoHunt API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Cache mémoire ────────────────────────────────────────────────
_cache = {
    "data": None,       # top 300 traités
    "meta": {},          # dominance BTC, total MCAP, Fear & Greed
    "ts": 0,             # timestamp dernier rafraîchissement
    "raw": None,         # snapshot brut pour historique
}
LOCK = asyncio.Lock()
WEBSOCKETS: set[WebSocket] = set()

# ── Helpers ──────────────────────────────────────────────────────

def rsi(prices: list[float], period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    gains, losses = 0.0, 0.0
    for i in range(-period, 0):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 1)

def sma(prices: list[float], period: int) -> float | None:
    if len(prices) < period:
        return None
    return round(sum(prices[-period:]) / period, 4)

def momentum(prices: list[float]) -> float | None:
    if len(prices) < 2:
        return None
    return round(((prices[-1] / prices[-2]) - 1) * 100, 2)

def trend_score(coin: dict, history: list) -> dict:
    """Calcule un score de tendance 0-100 plus le RSI/SMA."""
    prices = [h["price"] for h in history] if history else []
    score = 50  # neutre
    result = {"rsi_14": None, "sma_50": None, "sma_200": None, "momentum_24h": None, "trend_score": 50}

    if len(prices) >= 2:
        mom = (prices[-1] / prices[-2] - 1) * 100
        result["momentum_24h"] = round(mom, 2)
        if mom > 5:
            score += 15
        elif mom > 2:
            score += 8
        elif mom < -5:
            score -= 15
        elif mom < -2:
            score -= 8

    r = rsi(prices)
    result["rsi_14"] = r
    if r is not None:
        if r > 70:
            score -= 10  # surachat
        elif r < 30:
            score += 10  # survendu → potentiel rebond
        elif 40 <= r <= 60:
            score += 5   # sain

    s50 = sma(prices, 50)
    result["sma_50"] = s50
    s200 = sma(prices, 200)
    result["sma_200"] = s200
    if s50 and s200 and s50 > s200:
        score += 10  # golden cross
    elif s50 and s200 and s50 < s200:
        score -= 10  # death cross

    # Volume anormal
    if history:
        avg_vol = sum(h.get("volume", 0) or 0 for h in history) / len(history)
        current_vol = coin.get("volumeUsd", 0) or 0
        if avg_vol > 0 and current_vol > avg_vol * 2:
            score += 10
            result["volume_spike"] = True
        else:
            result["volume_spike"] = False

    result["trend_score"] = max(0, min(100, score))
    return result

# ── Fetch CoinCap ────────────────────────────────────────────────

async def fetch_coincap(client: httpx.AsyncClient, limit: int = 250) -> list[dict]:
    """Récupère les top coins depuis CoinCap."""
    try:
        resp = await client.get(f"https://api.coincap.io/v2/assets?limit={limit}", timeout=15)
        if resp.status_code == 200:
            return resp.json().get("data", [])
    except Exception:
        pass
    return []

async def fetch_coincap_history(client: httpx.AsyncClient, coin_id: str, interval: str = "h1") -> list[dict]:
    """Historique 7j pour les indicateurs techniques."""
    try:
        resp = await client.get(
            f"https://api.coincap.io/v2/assets/{coin_id}/history?interval={interval}",
            timeout=10
        )
        if resp.status_code == 200:
            return resp.json().get("data", [])
    except Exception:
        pass
    return []

# ── Fetch CoinGecko (complément) ────────────────────────────────

async def fetch_coingecko_global(client: httpx.AsyncClient) -> dict:
    """Dominance, Fear & Greed, total MCAP."""
    meta = {"btc_dominance": None, "eth_dominance": None, "total_mcap": None, "fear_greed": None}
    try:
        resp = await client.get("https://api.coingecko.com/api/v3/global", timeout=10)
        if resp.status_code == 200:
            d = resp.json().get("data", {})
            meta["total_mcap"] = d.get("total_market_cap", {}).get("usd")
            mcap_pct = d.get("market_cap_percentage", {})
            meta["btc_dominance"] = round(mcap_pct.get("btc", 0), 1)
            meta["eth_dominance"] = round(mcap_pct.get("eth", 0), 1)
    except Exception:
        pass
    # Fear & Greed
    try:
        resp = await client.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                meta["fear_greed"] = {"value": int(data[0].get("value", 50)), "classification": data[0].get("value_classification", "Neutral")}
    except Exception:
        pass
    return meta

# ── Mise à jour du cache ─────────────────────────────────────────

async def refresh_cache():
    async with httpx.AsyncClient() as client:
        try:
            assets = await fetch_coincap(client, 250)
            meta = await fetch_coingecko_global(client)

            if not assets:
                return

            assets.sort(key=lambda x: float(x.get("marketCapUsd", 0) or 0), reverse=True)

            # Option 1 : en parallèle, on fetch l'historique des top 100 seulement (pour la perf)
            top_100_ids = [a["id"] for a in assets[:100] if a.get("id")]
            tasks = [fetch_coincap_history(client, cid) for cid in top_100_ids]
            histories = await asyncio.gather(*tasks, return_exceptions=True)
            history_map = {}
            for cid, hist in zip(top_100_ids, histories):
                if isinstance(hist, list) and hist:
                    # Transformer pour analyse : prix et volume horaire
                    prices = [{"price": float(h["priceUsd"]), "volume": float(h.get("volumeUsd", 0) or 0), "time": h["time"]} for h in hist[-336:]]  # 14 jours max
                    history_map[cid] = prices

            processed = []
            for asset in assets:
                coin_id = asset.get("id", "")
                name = asset.get("name", "")
                symbol = asset.get("symbol", "")
                price = float(asset.get("priceUsd", 0) or 0)
                mcap = float(asset.get("marketCapUsd", 0) or 0)
                vol = float(asset.get("volumeUsd24Hr", 0) or 0)
                supply = float(asset.get("supply", 0) or 0)
                max_supply = asset.get("maxSupply")
                if max_supply is not None:
                    max_supply = float(max_supply)

                # Changement 24h
                change_24h = asset.get("changePercent24Hr")
                change_24h = round(float(change_24h), 2) if change_24h else None

                # VWAP 24h
                vwap = asset.get("vwap24Hr")
                vwap = round(float(vwap), 2) if vwap else None

                # Calcul volume/mcap ratio
                vol_mcap_ratio = round(vol / mcap, 4) if mcap > 0 else 0

                # Indicateurs techniques
                history = history_map.get(coin_id, [])
                indicators = trend_score(asset, history)

                coin = {
                    "id": coin_id,
                    "rank": asset.get("rank"),
                    "name": name,
                    "symbol": symbol.upper(),
                    "price": round(price, 8) if price < 0.01 else round(price, 4) if price < 1 else round(price, 2),
                    "price_raw": price,
                    "mcap": int(mcap),
                    "volume24h": int(vol),
                    "supply": int(supply),
                    "max_supply": max_supply,
                    "change24h": change_24h,
                    "vwap24h": vwap,
                    "vol_mcap_ratio": vol_mcap_ratio,
                    **indicators,
                }
                processed.append(coin)

            # Si on a BTC en premier, le retirer du top 300
            if processed and processed[0]["symbol"] == "BTC":
                btc = processed.pop(0)
                meta["btc"] = btc

            # Top 300 : prendre les 300 suivants
            top_300 = processed[:300]

            now = int(time.time())
            async with LOCK:
                _cache["data"] = top_300
                _cache["meta"] = meta
                _cache["ts"] = now
                _cache["raw"] = processed  # avec BTC

            print(f"[CRYPTOHUNT] Cache rafraîchi : {len(top_300)} coins, {datetime.now().isoformat()}")

            # Notifier les websockets
            payload = json.dumps({"type": "update", "coins": top_300, "meta": meta, "ts": now})
            dead = set()
            for ws in WEBSOCKETS:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            WEBSOCKETS -= dead

        except Exception as e:
            print(f"[CRYPTOHUNT] Erreur refresh : {e}")

# ── Background refresh toutes les 90s ────────────────────────────

@app.on_event("startup")
async def startup():
    await refresh_cache()
    asyncio.create_task(_periodic_refresh())

async def _periodic_refresh():
    while True:
        await asyncio.sleep(90)
        await refresh_cache()

# ── Routes ───────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"name": "CryptoHunt API", "version": "1.0", "endpoints": ["/api/top300", "/api/snapshot", "/api/health", "/ws"]}

@app.get("/api/health")
async def health():
    async with LOCK:
        age = int(time.time()) - _cache["ts"]
        return {"status": "ok", "coins_cached": len(_cache["data"] or []), "cache_age_seconds": age}

@app.get("/api/top300")
async def get_top300():
    async with LOCK:
        if not _cache["data"]:
            return JSONResponse({"error": "Cache pas encore prêt"}, status_code=503)
        return {
            "coins": _cache["data"],
            "meta": _cache["meta"],
            "ts": _cache["ts"],
            "count": len(_cache["data"]),
        }

@app.get("/api/snapshot")
async def get_snapshot():
    """Snapshot historique pour GitHub Actions."""
    async with LOCK:
        return {"ts": _cache["ts"], "coins": _cache["data"], "meta": _cache["meta"]}

# ── WebSocket ────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    WEBSOCKETS.add(ws)
    # Envoi immédiat des données actuelles
    async with LOCK:
        if _cache["data"]:
            payload = json.dumps({"type": "update", "coins": _cache["data"], "meta": _cache["meta"], "ts": _cache["ts"]})
            await ws.send_text(payload)
    try:
        while True:
            await ws.receive_text()  # keepalive
    except WebSocketDisconnect:
        WEBSOCKETS.discard(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
