# Empire Life

`Empire Life` is a modular Streamlit prototype for a consequence-driven life simulator. It is built around youth-to-adulthood progression, family pressure, temptation, relationships, money stress, and long-term outcomes.

## Run

From the repository root:

```powershell
py -m streamlit run empire_life/app.py --server.port 8502
```

The root project already depends on `streamlit`, so no separate requirements file is needed for this first prototype.

If PowerShell gives you trouble, use the batch launcher:

- double-click [launch_empire_life.bat](C:\Users\Aharp\OneDrive\Desktop\ai_parlay_builder_starter\ai_parlay_builder\launch_empire_life.bat)
- or run it from File Explorer and keep the terminal window open while you play

Then open [http://localhost:8502](http://localhost:8502).

## Current prototype scope

- childhood or teen starting point
- family background modifiers
- stage-specific decisions
- temptation, morality, discipline, debt, and legal risk
- relationship and family strain
- adult outcome checks

## Suggested next upgrades

- household members with names and traits
- more relationship events and romance arcs
- jobs, rent, and recurring bills
- district-specific opportunities and dangers
- save/load and balancing tables

## Deploy For Playtesting

The cleanest way to get a real public test link for your sister is to deploy `Empire Life` as its own Streamlit app.

### Files already prepared

- [app.py](C:\Users\Aharp\OneDrive\Desktop\ai_parlay_builder_starter\ai_parlay_builder\empire_life\app.py)
- [requirements.txt](C:\Users\Aharp\OneDrive\Desktop\ai_parlay_builder_starter\ai_parlay_builder\empire_life\requirements.txt)
- [render.yaml](C:\Users\Aharp\OneDrive\Desktop\ai_parlay_builder_starter\ai_parlay_builder\empire_life\render.yaml)

### Recommended path: Render

1. Push this repo to GitHub.
2. In Render, create a new Blueprint or Web Service from the repo.
3. Point Render at the `empire_life` app config:
   - easiest option: use [render.yaml](C:\Users\Aharp\OneDrive\Desktop\ai_parlay_builder_starter\ai_parlay_builder\empire_life\render.yaml)
4. Deploy.
5. Render will give you a public URL you can text to your sister.

### If Render asks for manual settings

- Root Directory: `empire_life`
- Build Command: `pip install -r requirements.txt`
- Start Command: `streamlit run app.py --server.address 0.0.0.0 --server.port $PORT`
- Python Version: `3.11.11`

### Important note

This deployment is for outside testing, not final release. It is intentionally lightweight so you can gather feedback quickly before investing in fuller production polish.
