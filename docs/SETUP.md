# Setup Guide

## Purpose

This guide is for an operator, buyer, or collaborator who needs to run AI Parlay Builder locally without reverse-engineering the repo.

## Requirements

- Windows machine or another environment that can run Python
- Python 3.11 recommended
- `pip`
- Streamlit
- Access to any provider keys you want to use for live sync

## Local install

From the project root:

```powershell
python -m pip install -r requirements.txt
```

## Local configuration

1. Copy [`.env.example`](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.env.example) to `.env`
2. Fill in the provider keys you want to use
3. Leave unused optional keys blank

## Run locally

Standard local run:

```powershell
python -m streamlit run app.py
```

If another local Streamlit app already uses port `8501`, run this app on a dedicated port:

```powershell
python -m streamlit run app.py --server.port 8503 --server.headless false
```

## Current local launcher convention

The user's local desktop shortcut is configured to launch AI Parlay Builder on:

- `http://localhost:8503`

This avoids conflict with another local Streamlit app already using `8501`.

## Recommended first-run checks

1. Confirm the app opens without import errors
2. Confirm `Sport`, `Board Type`, `Theme`, and `View` selectors render
3. Confirm `Overview` loads
4. Confirm `Results & Grading` opens
5. If using live sync, confirm the SportsGameOdds guard shows a valid-key state

## Local data modes

The app supports both:

- `Live` workflows backed by provider sync
- `Demo` workflows backed by local/synthetic/demo data

If API keys are unavailable, the app can still be demonstrated with demo workflows.

## Recommended operator habit

Use:

- `Simple` view for demoing or showing the product to buyers
- `Pro` view for tuning, diagnostics, and deeper review
