# Config Map

## Primary config sources

AI Parlay Builder currently relies on these main configuration layers:

1. [`.env`](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.env)
2. [`.env.example`](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.env.example)
3. [`.streamlit/secrets.example.toml`](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.streamlit/secrets.example.toml)
4. Streamlit session/view preferences inside the app

## Environment variables

### Required for live SportsGameOdds workflows

- `SPORTSGAMEODDS_API_KEY`

Without this key:

- live SportsGameOdds sync will not work
- usage guard checks will report local live-sync unavailability

### Optional provider keys

- `BALLDONTLIE_API_KEY`
- `ODDS_API_KEY`
- `PANDASCORE_API_KEY`
- `ABIOS_API_KEY`

These are not required for the current core demo/sportsbook flow, but they are part of the project scaffold and optional integrations.

### Database and storage

- `SQLITE_PATH`
- `DATABASE_URL`

Behavior:

- local/dev can use SQLite
- hosted deployments can use Postgres via `DATABASE_URL`

## SportsGameOdds usage controls

These variables control local/live sync safety:

- `SPORTSGAMEODDS_MIN_MONTHLY_ENTITIES_REMAINING`
- `SPORTSGAMEODDS_MIN_DAILY_ENTITIES_REMAINING`
- `SPORTSGAMEODDS_MINUTE_REQUEST_BUFFER`
- `SPORTSGAMEODDS_MAX_EVENTS_PER_LEAGUE_SYNC`
- `SPORTSGAMEODDS_SYNC_COOLDOWN_MINUTES`
- `SPORTSGAMEODDS_ONLY_FUTURE_EVENTS`
- `SPORTSGAMEODDS_FUTURE_WINDOW_HOURS`
- `SPORTSGAMEODDS_INCLUDE_NBA_EXOTICS`

These are especially important for a future owner because they reduce accidental overuse of a provider allocation.

## Streamlit secrets

Use Streamlit secrets for hosted deployment when the app is running on Streamlit Community Cloud.

Template:

- [`.streamlit/secrets.example.toml`](C:/Users/Aharp/OneDrive/Desktop/ai_parlay_builder_starter/ai_parlay_builder/.streamlit/secrets.example.toml)

## Product packaging controls

Inside the app, `View Preferences` now includes product packaging controls such as:

- `Product plan`
  - `Core`
  - `Pro`
  - `Owner`
- `Default app view`
  - `Simple`
  - `Pro`

These are presentation-layer controls that help package the app for different audiences.

## Sensitive files

Keep these private:

- `.env`
- database files
- any production credentials

Do not commit live credentials to Git.
