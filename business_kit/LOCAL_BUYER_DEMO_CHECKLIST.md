# AI Parlay Builder Local Buyer Demo Checklist

## Purpose

Use this checklist before showing AI Parlay Builder locally to a buyer, collaborator, or potential acquirer.

The goal is to make sure the app opens in its calmest, clearest posture and that the most valuable surfaces work without surprises.

## Before you launch

Confirm:

- local dependencies are already installed
- the app launches on the expected local port
- any live-provider keys you plan to rely on are valid
- you are ready to use demo-backed flows if live data looks noisy

If another local Streamlit app is using `8501`, launch AI Parlay Builder on:

- `8503`

## Launch checklist

1. start the app locally
2. open the correct local URL
3. wait for the page to fully render
4. do not begin the demo until the top controls and hero section are stable

## Reset to calm posture

Before the demo, open `View Preferences` and click:

- `Reset To Buyer Demo Defaults`

That should return the app to:

- `NBA`
- `Sportsbook`
- `Core` plan
- `Simple` view
- `Light` theme
- compact table views

## First screen check

Before you share your screen, confirm these are visible and correct:

- `Sport`
- `Board Type`
- `Theme`
- `View`
- hero section
- tabs render normally

If anything looks mid-workflow or overly deep, reset the buyer demo defaults again.

## Core walkthrough order

Use this sequence:

1. `Overview`
2. `Edge Scanner`
3. `Parlay Lab`
4. `Results & Grading`

Only open deeper `Pro` surfaces if the buyer asks for more detail.

## What to verify in Overview

Confirm:

- `Today's Operating Mode` renders
- `Workflow Snapshot` renders
- `Suggested Next Steps` renders
- `Notification Center` is readable

What you want the buyer to feel:

- the app is guided
- the workflow is organized
- the product has a clear story

## What to verify in Edge Scanner

Confirm:

- ranked edge rows are present
- filters render normally
- the page feels readable in `Simple` view

What you want the buyer to feel:

- the app reduces scanning workload
- the ranking layer is real

## What to verify in Parlay Lab

Confirm:

- smart profile controls load
- a ticket or build surface is visible
- stake planning renders without errors

What you want the buyer to feel:

- the product converts ranked candidates into a usable build workflow

## What to verify in Results & Grading

Confirm:

- workflow status cards render
- saved tickets or tracked workflow surfaces open normally
- ticket review area is readable

What you want the buyer to feel:

- the product has memory
- it supports review, not just selection

## If live data is weak

Use this line:

`The app can also be demonstrated cleanly with demo-backed workflows, which is useful because it reduces dependence on provider availability during a review or handoff.`

Do not force a fragile live demo if the board looks stale or noisy.

## If the buyer asks for more depth

Then switch to:

- `Pro` view

Briefly show:

- smart audit depth
- review and backtest surfaces
- owner/handoff packaging in `View Preferences`

Then return to the main story.

## What not to spend time on

Avoid getting stuck in:

- provider key setup
- obscure edge cases
- every tab in one session
- repo naming cleanup
- low-level code explanations unless the buyer asks

## Final 30-second wrap-up

End with:

`AI Parlay Builder combines scan, build, track, and learn inside one workflow product. It can be demoed calmly, reviewed deeply, and handed off with much less guesswork than a typical internal-only tool.`

## Supporting references

Pair this checklist with:

- [docs/DEMO_WALKTHROUGH.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/docs/DEMO_WALKTHROUGH.md)
- [business_kit/BUYER_ONE_SHEET.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/business_kit/BUYER_ONE_SHEET.md)
- [business_kit/PLAN_MATRIX.md](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/business_kit/PLAN_MATRIX.md)
