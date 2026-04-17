# Empire Life Deploy Checklist

Use this to get a public playtest link for `Empire Life` quickly.

## Repository

- GitHub repo already configured in `.git/config`:
  - `origin`: `https://github.com/hailtothief3-byte/ai_parlay_builder.git`

## What to push

The key files for the playtest deployment are:

- `empire_life/app.py`
- `empire_life/requirements.txt`
- `empire_life/render.yaml`
- `empire_life/systems/*`

## Fastest push path

If you use GitHub Desktop:

1. Open the repo in GitHub Desktop.
2. Review changes in the `empire_life` folder.
3. Commit with a message like:
   - `Prepare Empire Life for external playtesting`
4. Push to `origin/main`.

## Render setup

Once the repo is pushed:

1. Open [Render](https://render.com).
2. Create a new Web Service or Blueprint from the GitHub repo.
3. If using manual settings, set:
   - Root Directory: `empire_life`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `streamlit run app.py --server.address 0.0.0.0 --server.port $PORT`
   - Python Version: `3.11.11`
4. Deploy.
5. Copy the public Render URL.

## What to test first

Ask testers to check:

- Is the game easy to understand on first load?
- Do the choices feel meaningful?
- Does the run start slow, interesting, or confusing?
- Which path feels strongest: structured, risky, family, or community?
- At what point does the game become engaging or repetitive?

## Recommended note

Send the public URL with the tester note in:

- `empire_life/TESTER_NOTE.md`
