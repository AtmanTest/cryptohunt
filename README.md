# 🦊 CryptoHunt

Dashboard crypto temps réel — top 300 altcoins (après BTC) triés par market cap, avec indicateurs techniques (RSI, SMA, Momentum, Trend Score), mise à jour live via WebSocket et snapshots historiques automatiques.

## Architecture

| Composant | Technologie | Hébergement | Coût |
|-----------|------------|-------------|------|
| Backend API | FastAPI (Python) | Render Free | 0€ |
| Frontend | Vanilla JS + WebSocket | GitHub Pages | 0€ |
| Snapshots historiques | GitHub Actions (toutes les 30min) | GitHub | 0€ |
| Keep-alive | cron-job.org (toutes les 5min) | cron-job.org | 0€ |

## Endpoints

- `GET /api/top300` — Top 300 altcoins avec indicateurs
- `GET /api/snapshot` — Snapshot brut pour archivage
- `WS /ws` — WebSocket mise à jour live (toutes les 90s)
- `GET /api/health` — Health check

## Déploiement

1. Créer le repo GitHub
2. Connecter Render au repo → Web Service
3. Activer GitHub Pages sur la branche main, dossier / (root)
4. Créer un job cron-job.org pointant sur https://cryptohunt.onrender.com/api/health toutes les 5 min

## Données

- Prix, MCAP, volume : CoinCap API
- Dominance, total MCAP : CoinGecko
- Fear & Greed Index : Alternative.me
- Historique : CoinCap (7 jours, horaire)
