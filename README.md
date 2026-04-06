# AI-Powered Parlay + Player Prop Builder

This is a modular Python + Streamlit starter project for a sports research platform that can:
- rank player props
- generate parlays
- create DFS pick'em cards
- format sportsbook-style slips
- support mainstream sports plus esports models for CS2, LoL, and DOTA2
- include higher-variance markets like first basket scorer and home run hitters

## Important Note
This starter is positioned as a **research and decision-support tool**. It does not automate bet placement or guarantee outcomes.

## Included Modules
- `app.py` — Streamlit interface
- `config.py` — app configuration
- `data/sample_data.py` — synthetic slate generator for local demo/testing
- `features/engine.py` — feature scoring logic
- `models/predictors.py` — projection and confidence engine
- `builders/parlays.py` — parlay assembly logic
- `builders/dfs_cards.py` — DFS card builder
- `builders/slips.py` — formatted slip output
- `services/research.py` — service layer orchestration
- `utils/formatting.py` — table formatting helpers

## Sports / markets scaffolded
- NBA: points, rebounds, assists, PRA, first basket
- MLB: hits, total bases, home run, pitcher strikeouts
- NFL: passing/rushing/receiving markets
- NHL: shots, points, anytime goal
- CS2: kills, headshots, match winner
- LoL: kills, assists, kills+assists, team winner
- DOTA2: kills, assists, fantasy score, map winner

## Run locally
```bash
pip install -r requirements.txt
python -m streamlit run app.py
```

## Deploy
This app is now set up for hosted deployment with persistent SQLite storage.

### Recommended path: Render
Why Render:
- supports Python web services cleanly
- supports persistent disks, which matters because this app stores SQLite data and sync state locally
- easy to redeploy as the app keeps evolving

Files included for deployment:
- `.streamlit/config.toml`
- `render.yaml`
- `db/session.py` now supports `DATABASE_URL` or `SQLITE_PATH`

### Render deployment steps
1. Push this project to GitHub.
2. In Render, create a new Blueprint from the repo.
3. Render will detect `render.yaml`.
4. Add your environment variables in Render:
   - `SPORTSGAMEODDS_API_KEY`
   - optional: `BALLDONTLIE_API_KEY`
   - optional: `ODDS_API_KEY`
   - optional: `PANDASCORE_API_KEY`
   - optional: `ABIOS_API_KEY`
5. Deploy the service.

The deployment is configured to persist the SQLite database at:
- `/var/data/parlay_builder.db`

### Can you still keep building after deployment?
Yes.

Deployment does not freeze the app. You can keep:
- editing code locally
- adding new tabs, models, providers, and UI changes
- pushing updates to GitHub
- redeploying the hosted app

Your normal workflow becomes:
1. change code locally
2. test locally
3. push to GitHub
4. redeploy or let Render auto-deploy

### One important note
Because this app currently uses SQLite, the persistent disk matters. If you later want:
- multi-user access
- safer concurrent writes
- larger production scale

the next step would be moving from SQLite to Postgres.

## Free deployment path
If you want to avoid paid persistent disks, the recommended free-friendly path is:
- host the app on Streamlit Community Cloud
- use a free hosted Postgres database

Why this works:
- the app now supports `DATABASE_URL`
- sync metadata no longer depends only on local JSON files
- the app can read secrets from Streamlit secrets

### Recommended stack
- app host: Streamlit Community Cloud
- database: a free hosted Postgres provider such as Neon or Supabase

### Setup summary
1. Create a free Postgres database.
2. Copy its connection string.
3. In Streamlit Community Cloud, deploy this GitHub repo.
4. In the app secrets, add:
   - `DATABASE_URL`
   - `SPORTSGAMEODDS_API_KEY`
   - optional other API keys

Use [`.streamlit/secrets.example.toml`](/C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.streamlit/secrets.example.toml) as your template.

### Important note
Debug files and local sample files can still be ephemeral in free hosting. That is okay.
The important persistence is now the database.

## Publish to GitHub
Before pushing this repo:
- keep `.env` private
- keep `parlay_builder.db` private
- keep local debug files private

This project now includes:
- `.gitignore` rules for local secrets, SQLite DB, sync state, and debug JSON files
- `.env.example` as a safe template for environment variables

### Recommended GitHub setup
1. Install GitHub Desktop or Git for Windows.
2. Create a new GitHub repository.
3. Add this project folder to GitHub Desktop, or initialize Git from the terminal once Git is installed.
4. Commit the project.
5. Push to GitHub.

### If Git is not installed yet
On this machine, `git` may not be available on the command line yet. If that is still true, the easiest path is:
- install [GitHub Desktop](https://desktop.github.com/)
- or install [Git for Windows](https://git-scm.com/download/win)

After that, this folder should be ready to publish.

## Recommended next upgrades
1. Replace synthetic data with real historical game logs and odds feeds
2. Add database persistence with SQLite/Postgres
3. Add backtesting by market type
4. Add bankroll tracking and CLV tracking
5. Add separate models per sport/market
6. Add calibration and feature importance views
7. Add CSV imports for sportsbook and DFS boards

## Real-data build path
For a production-grade version, break the project into sport-specific pipelines:
- `nba_model.py`
- `mlb_model.py`
- `esports_model.py`

Then train per market:
- first basket scorer probability model
- home run hitter model
- pitcher strikeout model
- CS2 map/player kills model
- LoL kills/assists and team-side model
- DOTA2 kill/fantasy score model
