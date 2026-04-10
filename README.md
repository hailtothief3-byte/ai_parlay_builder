# AI Parlay Builder

AI Parlay Builder is a Streamlit-based workflow and intelligence platform for:

- scanning prop markets
- ranking edges
- building sportsbook parlays
- packaging DFS pick'em slips
- tracking tickets and bankroll flow
- learning from graded history

This product is positioned as a **research and decision-support tool**. It does not automate wager placement or guarantee outcomes.

## Current operating model

The app now supports packaging layers for different audiences:

- `Simple` view for calmer demos and day-to-day operation
- `Pro` view for deeper diagnostics and tuning
- product-plan posture:
  - `Core`
  - `Pro`
  - `Owner`

## Start here

- Setup: [docs/SETUP.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/docs/SETUP.md)
- Config map: [docs/CONFIG.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/docs/CONFIG.md)
- Deployment notes: [docs/DEPLOYMENT.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/docs/DEPLOYMENT.md)
- Feature map: [docs/FEATURE_MAP.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/docs/FEATURE_MAP.md)
- Buyer/operator handoff: [docs/BUYER_HANDOFF.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/docs/BUYER_HANDOFF.md)

## Local run

```powershell
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

If `8501` is already occupied by another local Streamlit app, run AI Parlay Builder on `8503` instead:

```powershell
python -m streamlit run app.py --server.port 8503 --server.headless false
```

## Current production URL

- [https://ai-parlay-builder.streamlit.app](https://ai-parlay-builder.streamlit.app)

## Configuration templates

- [`.env.example`](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.env.example)
- [`.streamlit/secrets.example.toml`](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.streamlit/secrets.example.toml)

## Repo note

This repo still contains some legacy starter-era file names and scaffolded modules. The app itself is functional and actively evolved, but a future cleanup pass would improve consistency for a long-term owner.
