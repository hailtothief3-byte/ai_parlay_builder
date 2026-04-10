# Deployment Notes

## Current hosted target

The current production app is hosted on Streamlit Community Cloud:

- [https://ai-parlay-builder.streamlit.app](https://ai-parlay-builder.streamlit.app)

## Current deploy behavior

- Production deploys from GitHub
- Changes pushed to `main` are expected to redeploy automatically
- If a deploy stalls or boots into a stale state, use `Manage app` and `Reboot app` in Streamlit Cloud

## Streamlit Community Cloud deployment checklist

1. Push the latest code to GitHub
2. Confirm `app.py` is the entrypoint
3. Add required secrets in Streamlit Cloud
4. Reboot the app if the deployment appears stale

## Required hosted secrets

At minimum for live SportsGameOdds workflows:

- `DATABASE_URL` or a suitable storage configuration
- `SPORTSGAMEODDS_API_KEY`

Optional:

- `BALLDONTLIE_API_KEY`
- `ODDS_API_KEY`
- `PANDASCORE_API_KEY`
- `ABIOS_API_KEY`

## Render configuration

The repo still contains [render.yaml](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/render.yaml), which documents a Render-compatible deployment path with persistent disk support.

This is useful if a future owner wants:

- a non-Streamlit host
- persistent local SQLite storage
- a more traditional hosted web-service setup

## Recommended production posture

For a future owner or buyer, the cleanest hosted pattern is:

1. Streamlit Community Cloud for the app
2. Postgres for persistence
3. hosted secrets for all provider keys

This reduces dependence on ephemeral local files and makes transfer cleaner.

## Common recovery actions

### Production import error

If the hosted app shows an import error:

1. confirm the latest commit is on GitHub
2. wait for redeploy
3. use `Manage app` -> `Reboot app`

### Local app opens the wrong Streamlit project

If another app is already using `8501`, launch AI Parlay Builder on:

- `8503`

### Invalid API key warning

If the app says the SportsGameOdds key is invalid or expired:

1. update `.env` locally or hosted secrets in production
2. restart local app or reboot hosted app

## Recommended operator workflow

1. edit locally
2. test locally
3. push to GitHub
4. confirm Streamlit production redeploys
5. smoke-test:
   - Overview
   - Edge Scanner
   - Parlay Lab
   - Results & Grading
