import pandas as pd
import streamlit as st
import json
from pathlib import Path

from builders.parlays import ParlaySettings, build_parlay
from config import CONFIG
from db import init_db
from ingestion.providers import get_provider
from sports_config import get_market_coverage, get_market_coverage_map, get_sport_config, get_sport_labels, get_sport_provider_name, is_live_sync_enabled, resolve_live_keys_for_label
from services.demo_seed import clear_demo_live_data, seed_all_demo_live_data, seed_demo_live_data
from services.analytics import build_calibration_summary, build_clv_backtest, build_ticket_benchmark_summary, build_true_backtest, build_true_calibration_summary, build_true_confidence_summary, build_true_market_summary, build_true_sportsbook_summary
from services.board_service import get_latest_board
from services.bankroll_service import annotate_stake_recommendations, recommend_parlay_stake
from services.bankroll_journal_service import add_journal_entry, build_bankroll_kpis, build_bankroll_summary, get_journal_entries, settle_journal_entry, sync_ticket_journal_entries
from services.edge_scanner import scan_edges
from builders.prop_cards import build_prop_cards
from services.history_service import get_history_suggestions, get_line_history
from services.projection_builder import build_live_projections_for_sports
from services.results_service import (
    get_graded_picks,
    get_prop_results,
    get_tracked_picks,
    get_unresolved_tracked_picks,
    import_prop_results_csv,
    import_tracked_picks_csv,
    sync_prop_results_from_sportsgameodds,
    track_edge_rows,
    upsert_prop_result,
)
from services.research import ResearchService
from services.settings_manager import reload_runtime_modules, upsert_env_values
from services.stats_service import (
    build_stats_template,
    get_latest_stats_snapshots,
    import_stats_csv,
    sync_stats_from_balldontlie,
    sync_stats_from_sportsgameodds,
)
from services.sync_policy import get_last_sync, get_sync_payload
from services.ticket_service import export_ticket_legs_for_csv, get_saved_tickets, get_ticket_legs, get_ticket_legs_with_results, get_ticket_summary_with_grades, import_ticket_legs_csv, save_ticket
from services.usage_guard import estimate_sportsgameodds_sync_cost, safe_get_sportsgameodds_usage_summary

NBA_EXOTIC_DEBUG_PATH = "data/sportsgameodds_nba_exotics_debug.json"


def load_nba_exotic_debug() -> dict:
    path = Path(NBA_EXOTIC_DEBUG_PATH)
    if not path.exists():
        return {}

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def apply_market_coverage(df: pd.DataFrame, coverage_map: dict[str, dict[str, str]]) -> pd.DataFrame:
    if df.empty or "market" not in df.columns:
        return df

    enriched = df.copy()
    enriched["coverage_status"] = enriched["market"].map(
        lambda market: coverage_map.get(
            market,
            {
                "status": "Provider Unavailable",
                "note": "No market coverage metadata is configured for this market.",
            },
        )["status"]
    )
    enriched["coverage_note"] = enriched["market"].map(
        lambda market: coverage_map.get(
            market,
            {
                "status": "Provider Unavailable",
                "note": "No market coverage metadata is configured for this market.",
            },
        )["note"]
    )
    return enriched


def render_coverage_badge(status: str) -> str:
    badge_styles = {
        "Live": ("#0f5132", "#d1e7dd"),
        "Demo Only": ("#664d03", "#fff3cd"),
        "Provider Unavailable": ("#842029", "#f8d7da"),
    }
    text_color, bg_color = badge_styles.get(status, ("#1f2937", "#e5e7eb"))
    return (
        "<span style="
        f"'display:inline-block;padding:0.2rem 0.55rem;border-radius:999px;"
        f"font-size:0.78rem;font-weight:600;background:{bg_color};color:{text_color};'"
        f">{status}</span>"
    )


def style_coverage_table(df: pd.DataFrame):
    if df.empty or "coverage_status" not in df.columns:
        return df

    style_map = {
        "Live": "background-color: #d1e7dd; color: #0f5132; font-weight: 600;",
        "Demo Only": "background-color: #fff3cd; color: #664d03; font-weight: 600;",
        "Provider Unavailable": "background-color: #f8d7da; color: #842029; font-weight: 600;",
    }

    def coverage_style(value):
        return style_map.get(str(value), "")

    return df.style.map(coverage_style, subset=["coverage_status"])


def filter_dataframe(
    df: pd.DataFrame,
    market_key: str = "",
    player_query: str = "",
    sort_by: str | None = None,
    ascending: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()
    if market_key and "market" in filtered.columns:
        filtered = filtered[filtered["market"] == market_key].copy()
    if player_query and "player" in filtered.columns:
        filtered = filtered[
            filtered["player"].astype(str).str.contains(player_query, case=False, na=False)
        ].copy()
    if sort_by and sort_by in filtered.columns:
        filtered = filtered.sort_values(sort_by, ascending=ascending)
    return filtered


def render_shell_header(sport_label: str, provider: str, board_type: str, sync_enabled: bool, last_sync) -> None:
    sync_text = "Live sync enabled" if sync_enabled else "Demo-backed live views"
    last_sync_text = (
        last_sync.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        if last_sync is not None
        else "No recent sync"
    )
    st.markdown(
        f"""
        <div class="app-hero">
            <div class="app-hero__eyebrow">AI Parlay Builder</div>
            <div class="app-hero__title">Sharper prop workflows for {sport_label}</div>
            <div class="app-hero__subtitle">
                Live odds, projection building, grading, bankroll tracking, and ticket planning in one workspace.
            </div>
            <div class="app-hero__meta">
                <span class="hero-pill">Provider: {provider}</span>
                <span class="hero-pill">Board: {board_type}</span>
                <span class="hero-pill">{sync_text}</span>
                <span class="hero-pill">Last sync: {last_sync_text}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_header(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="section-header">
            <div class="section-header__title">{title}</div>
            <div class="section-header__subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_prop_card(card: dict) -> None:
    accent_map = {
        "Live": "#2a9d8f",
        "Demo Only": "#e9c46a",
        "Provider Unavailable": "#e76f51",
    }
    accent = accent_map.get(card.get("coverage_status"), "#264653")
    st.markdown(
        f"""
        <div style="
            background: rgba(255,255,255,0.86);
            border: 1px solid rgba(31,41,55,0.08);
            border-left: 6px solid {accent};
            border-radius: 18px;
            padding: 1rem 1rem 0.9rem 1rem;
            box-shadow: 0 12px 28px rgba(15,23,42,0.06);
            margin-bottom: 0.85rem;
        ">
            <div style="display:flex;justify-content:space-between;gap:1rem;align-items:flex-start;">
                <div>
                    <div style="font-size:1.03rem;font-weight:800;color:#1f2937;">{card['title']}</div>
                    <div style="font-size:0.9rem;color:#6b7280;margin-top:0.15rem;">{card['sportsbook']} • {card['pick']}</div>
                </div>
                <div>{render_coverage_badge(card["coverage_status"])}</div>
            </div>
            <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0.65rem;margin-top:0.85rem;">
                <div><div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;">Projection</div><div style="font-size:1rem;font-weight:700;">{card['projection']}</div></div>
                <div><div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;">Model Prob</div><div style="font-size:1rem;font-weight:700;">{card['model_prob']}%</div></div>
                <div><div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;">Edge</div><div style="font-size:1rem;font-weight:700;">{card['edge']}%</div></div>
                <div><div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;">Stake</div><div style="font-size:1rem;font-weight:700;">{card['recommended_units']}u</div></div>
            </div>
            <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:0.65rem;margin-top:0.75rem;">
                <div><div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;">Implied Prob</div><div style="font-size:0.96rem;font-weight:600;">{card['implied_prob']}%</div></div>
                <div><div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;">Consensus</div><div style="font-size:0.96rem;font-weight:600;">{card['consensus_line']}</div></div>
                <div><div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;">Confidence</div><div style="font-size:0.96rem;font-weight:600;">{card['confidence']}</div></div>
            </div>
            <div style="margin-top:0.7rem;font-size:0.86rem;color:#4b5563;">{card.get("coverage_note") or ""}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def compact_numeric_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    formatted = df.copy()
    for column in formatted.columns:
        if pd.api.types.is_float_dtype(formatted[column]):
            formatted[column] = formatted[column].round(3)
    return formatted

init_db()

st.set_page_config(page_title="AI Parlay Builder", layout="wide")
st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(244, 162, 97, 0.14), transparent 28%),
            radial-gradient(circle at top right, rgba(42, 157, 143, 0.12), transparent 26%),
            linear-gradient(180deg, #f6f4ee 0%, #fcfbf8 100%);
    }
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 3rem;
        max-width: 1400px;
    }
    .app-hero {
        padding: 1.5rem 1.6rem;
        border-radius: 24px;
        background: linear-gradient(135deg, rgba(24, 35, 52, 0.94), rgba(34, 63, 95, 0.92));
        color: #f8fafc;
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 20px 50px rgba(15, 23, 42, 0.18);
        margin-bottom: 1rem;
    }
    .app-hero__eyebrow {
        text-transform: uppercase;
        letter-spacing: 0.16em;
        font-size: 0.72rem;
        color: #f4a261;
        margin-bottom: 0.45rem;
        font-weight: 700;
    }
    .app-hero__title {
        font-size: 2rem;
        line-height: 1.05;
        font-weight: 800;
        margin-bottom: 0.45rem;
    }
    .app-hero__subtitle {
        font-size: 1rem;
        max-width: 760px;
        color: rgba(248, 250, 252, 0.84);
        margin-bottom: 1rem;
    }
    .app-hero__meta {
        display: flex;
        flex-wrap: wrap;
        gap: 0.55rem;
    }
    .hero-pill {
        display: inline-block;
        padding: 0.4rem 0.7rem;
        border-radius: 999px;
        background: rgba(255,255,255,0.08);
        border: 1px solid rgba(255,255,255,0.09);
        font-size: 0.84rem;
    }
    .section-header {
        margin: 0.1rem 0 0.9rem;
    }
    .section-header__title {
        font-size: 1.35rem;
        font-weight: 800;
        color: #1f2937;
        margin-bottom: 0.15rem;
    }
    .section-header__subtitle {
        font-size: 0.95rem;
        color: #6b7280;
    }
    [data-testid="stMetric"] {
        background: rgba(255,255,255,0.74);
        border: 1px solid rgba(31, 41, 55, 0.08);
        padding: 0.9rem 1rem;
        border-radius: 18px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
    }
    [data-testid="stDataFrame"] {
        background: rgba(255,255,255,0.72);
        border-radius: 18px;
        padding: 0.15rem;
        border: 1px solid rgba(31, 41, 55, 0.06);
    }
    [data-testid="stDataFrame"] div[role="grid"] {
        font-size: 0.92rem;
    }
    [data-testid="stDataFrame"] [role="columnheader"] {
        letter-spacing: 0.03em;
        font-weight: 700;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248, 246, 241, 0.98));
        border-right: 1px solid rgba(31, 41, 55, 0.06);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.35rem;
        background: rgba(255,255,255,0.66);
        padding: 0.45rem;
        border-radius: 18px;
        border: 1px solid rgba(31, 41, 55, 0.06);
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 14px;
        padding: 0.45rem 0.9rem;
        font-weight: 700;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #264653, #2a9d8f);
        color: white;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

selector_col1, selector_col2 = st.columns([1.2, 1.0])
sport_label = selector_col1.selectbox("Sport", get_sport_labels())
board_type = selector_col2.selectbox("Board Type", ["Sportsbook", "DFS"])

sport_config = get_sport_config(sport_label)
live_sport_keys = resolve_live_keys_for_label(sport_label)
sport_provider = get_sport_provider_name(sport_label)
sync_enabled = is_live_sync_enabled(sport_label)
market_coverage_df = pd.DataFrame(get_market_coverage(sport_label))
market_coverage_map = get_market_coverage_map(sport_label)

is_dfs = board_type == "DFS"
history_suggestions = get_history_suggestions(live_sport_keys, is_dfs=is_dfs)
sgo_usage = safe_get_sportsgameodds_usage_summary()
sgo_sync_estimate = estimate_sportsgameodds_sync_cost(sport_label) if sport_provider == "sportsgameodds" else None
last_sync = get_last_sync("sportsgameodds", sport_label) if sport_provider == "sportsgameodds" else None
render_shell_header(sport_label, sport_provider, board_type, sync_enabled, last_sync)
if not sync_enabled:
    st.info("This sport is routed through the esports provider slot. Demo/live-seeded views work now; external esports API integration is the next step.")

with st.expander("Market Coverage", expanded=False):
    if market_coverage_df.empty:
        st.caption("No market coverage metadata is configured yet.")
    else:
        st.dataframe(market_coverage_df, use_container_width=True, hide_index=True)

with st.sidebar:
    st.subheader("Demo Live Data")
    st.caption("Populate the live tabs with local sample events, odds, projections, and line-history snapshots.")

    st.divider()
    st.subheader("SportsGameOdds Guard")
    if sgo_usage.get("enabled"):
        if sgo_usage.get("ok_to_sync"):
            st.success(sgo_usage.get("message"))
        else:
            st.warning(sgo_usage.get("message"))

        usage_lines = []
        if sgo_usage.get("tier"):
            usage_lines.append(f"Tier: {sgo_usage['tier']}")
        if sgo_usage.get("minute_requests_remaining") is not None:
            usage_lines.append(f"Minute requests remaining: {sgo_usage['minute_requests_remaining']}")
        if sgo_usage.get("day_entities_remaining") is not None:
            usage_lines.append(f"Day entities remaining: {sgo_usage['day_entities_remaining']}")
        if sgo_usage.get("month_entities_remaining") is not None:
            usage_lines.append(f"Month entities remaining: {sgo_usage['month_entities_remaining']}")
        if usage_lines:
            st.caption(" | ".join(usage_lines))
    else:
        st.caption(sgo_usage.get("message"))

    if sport_provider == "sportsgameodds":
        st.caption(
            f"Per-sync cap: {CONFIG.sportsgameodds_max_events_per_league_sync} events. "
            f"Cooldown: {CONFIG.sportsgameodds_sync_cooldown_minutes} minutes."
        )
        st.caption(
            f"Future-only sync: {CONFIG.sportsgameodds_only_future_events}. "
            f"Window: next {CONFIG.sportsgameodds_future_window_hours} hours."
        )
        if sgo_sync_estimate:
            st.caption(
                f"Estimated cost for one {sport_label} sync: up to about {sgo_sync_estimate['estimated_entities']} entities "
                f"across {sgo_sync_estimate['max_events']} events."
            )

        with st.expander("Sync Settings", expanded=False):
            max_events = st.slider(
                "Max events per sync",
                min_value=1,
                max_value=20,
                value=CONFIG.sportsgameodds_max_events_per_league_sync,
                key="settings_max_events",
            )
            cooldown_minutes = st.slider(
                "Cooldown (minutes)",
                min_value=5,
                max_value=180,
                value=CONFIG.sportsgameodds_sync_cooldown_minutes,
                step=5,
                key="settings_cooldown",
            )
            future_only = st.checkbox(
                "Only sync future events",
                value=CONFIG.sportsgameodds_only_future_events,
                key="settings_future_only",
            )
            future_window = st.slider(
                "Future window (hours)",
                min_value=6,
                max_value=168,
                value=CONFIG.sportsgameodds_future_window_hours,
                step=6,
                key="settings_future_window",
            )
            include_nba_exotics = st.checkbox(
                "Enable NBA exotic market discovery",
                value=CONFIG.sportsgameodds_include_nba_exotics,
                key="settings_nba_exotics",
                help="Opt-in because exotic discovery may consume more SportsGameOdds entities.",
            )

            if st.button("Save Sync Settings", use_container_width=True):
                upsert_env_values(
                    {
                        "SPORTSGAMEODDS_MAX_EVENTS_PER_LEAGUE_SYNC": str(max_events),
                        "SPORTSGAMEODDS_SYNC_COOLDOWN_MINUTES": str(cooldown_minutes),
                        "SPORTSGAMEODDS_ONLY_FUTURE_EVENTS": str(future_only).lower(),
                        "SPORTSGAMEODDS_FUTURE_WINDOW_HOURS": str(future_window),
                        "SPORTSGAMEODDS_INCLUDE_NBA_EXOTICS": str(include_nba_exotics).lower(),
                    }
                )
                st.success("Saved sync settings to .env.")

            if st.button("Reload App Config", use_container_width=True):
                reload_runtime_modules()
                st.rerun()

        if st.button(f"Sync Live {sport_label} Now", use_container_width=True):
            provider = get_provider("sportsgameodds")
            sync_result = provider.sync_events_for_labels([sport_label])
            live_projection_counts = build_live_projections_for_sports([sport_label]) if sport_label in {"NBA", "MLB"} else {}
            refreshed_board = get_latest_board(live_sport_keys, is_dfs=False) if live_sport_keys else pd.DataFrame()
            first_basket_rows = 0
            if sport_label == "NBA" and not refreshed_board.empty:
                first_basket_rows = int((refreshed_board["market"] == "player_first_basket").sum())
            if sync_result.props_count > 0:
                st.success(
                    f"Synced {sport_label}: {sync_result.events_count} events, {sync_result.props_count} market rows, "
                    f"and {live_projection_counts.get(sport_label, 0)} live projections."
                )
                if sport_label == "NBA" and CONFIG.sportsgameodds_include_nba_exotics:
                    if first_basket_rows > 0:
                        st.info(f"Detected {first_basket_rows} live `player_first_basket` rows after the NBA sync.")
                    else:
                        st.warning(
                            "NBA exotic discovery is enabled, but no `player_first_basket` rows landed in the board. "
                            f"Check `{NBA_EXOTIC_DEBUG_PATH}` for the returned odd IDs and market names."
                        )
            elif sync_result.messages:
                st.warning(sync_result.messages[-1])
            else:
                st.info(f"No new {sport_label} rows were synced.")

        if sport_label == "NBA" and CONFIG.sportsgameodds_include_nba_exotics:
            exotic_debug = load_nba_exotic_debug()
            current_board = get_latest_board(live_sport_keys, is_dfs=False) if live_sport_keys else pd.DataFrame()
            current_first_basket_rows = 0
            if not current_board.empty:
                current_first_basket_rows = int((current_board["market"] == "player_first_basket").sum())

            st.divider()
            st.subheader("NBA Exotic Status")
            if current_first_basket_rows > 0:
                st.success(f"`player_first_basket` is present with {current_first_basket_rows} live rows.")
            else:
                st.warning("No live `player_first_basket` rows are currently in the board.")

            if exotic_debug:
                candidate_count = int(exotic_debug.get("candidate_market_count", 0) or 0)
                normalized_counts = exotic_debug.get("normalized_market_counts", {}) or {}
                debug_first_basket_rows = int(normalized_counts.get("player_first_basket", 0) or 0)

                st.caption(
                    f"Exotic candidates seen in last NBA debug scan: {candidate_count}. "
                    f"Normalized first-basket rows: {debug_first_basket_rows}."
                )

                with st.expander("Returned Exotic Candidates", expanded=False):
                    candidate_markets = pd.DataFrame(exotic_debug.get("candidate_markets", []))
                    if candidate_markets.empty:
                        st.caption("No first/score/basket-style candidate markets were found in the last debug scan.")
                    else:
                        st.dataframe(candidate_markets, use_container_width=True)

                with st.expander("Distinct Stat IDs", expanded=False):
                    stat_ids = exotic_debug.get("distinct_stat_ids", [])
                    if stat_ids:
                        st.code(", ".join(stat_ids))
                    else:
                        st.caption("No stat IDs recorded.")

                with st.expander("Distinct Market Names", expanded=False):
                    market_names = exotic_debug.get("distinct_market_names", [])
                    if market_names:
                        st.code(", ".join(market_names[:100]))
                    else:
                        st.caption("No market names recorded.")
            else:
                st.caption("Run `Sync Live NBA Now` with exotics enabled to populate the NBA exotic debug panel.")

    if st.button(f"Seed {sport_label} Demo Data", use_container_width=True):
        stats = seed_demo_live_data(sport_label)
        st.success(
            f"Seeded {sport_label}: {stats['events']} events, {stats['lines']} lines, {stats['projections']} projections."
        )

    if st.button("Seed All Demo Sports", use_container_width=True):
        seeded = seed_all_demo_live_data()
        total_events = sum(item["events"] for item in seeded.values())
        total_lines = sum(item["lines"] for item in seeded.values())
        total_projections = sum(item["projections"] for item in seeded.values())
        st.success(
            f"Seeded all demo sports: {total_events} events, {total_lines} lines, {total_projections} projections."
        )

    if st.button(f"Clear {sport_label} Demo Data", use_container_width=True):
        removed = clear_demo_live_data(sport_label)
        st.info(f"Removed {removed} seeded demo records for {sport_label}.")

    st.divider()
    st.subheader("Bankroll")
    bankroll_amount = st.number_input(
        "Bankroll ($)",
        min_value=50.0,
        max_value=100000.0,
        value=float(st.session_state.get("bankroll_amount", 1000.0)),
        step=50.0,
        key="bankroll_amount",
    )
    unit_size = st.number_input(
        "Unit size ($)",
        min_value=1.0,
        max_value=10000.0,
        value=float(st.session_state.get("unit_size", 25.0)),
        step=1.0,
        key="unit_size",
    )
    fractional_kelly = st.slider(
        "Kelly fraction",
        min_value=0.05,
        max_value=0.50,
        value=float(st.session_state.get("fractional_kelly", 0.25)),
        step=0.05,
        key="fractional_kelly",
    )
    max_bet_units = st.slider(
        "Max units per pick",
        min_value=0.5,
        max_value=5.0,
        value=float(st.session_state.get("max_bet_units", 3.0)),
        step=0.5,
        key="max_bet_units",
    )

    st.divider()
    st.subheader("Available Samples")
    if history_suggestions["players"] or history_suggestions["markets"]:
        if history_suggestions["players"]:
            st.caption("Players")
            st.code(", ".join(history_suggestions["players"]))
        if history_suggestions["markets"]:
            st.caption("Markets")
            st.code(", ".join(history_suggestions["markets"]))
    else:
        st.caption("Seed demo data to see available player and market suggestions here.")

tab0, tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Overview", "Live Board", "Edge Scanner", "Parlay Lab", "Line History", "Results & Grading", "Backtest", "Stats Import"])

with tab0:
    render_section_header("Overview", "A quick read on live data, model activity, bankroll state, and ticket flow.")
    overview_board = get_latest_board(live_sport_keys, is_dfs=is_dfs) if live_sport_keys else pd.DataFrame()
    overview_edges = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs) if live_sport_keys else pd.DataFrame()
    overview_edges = apply_market_coverage(overview_edges, market_coverage_map) if not overview_edges.empty else overview_edges
    overview_edges = annotate_stake_recommendations(
        overview_edges,
        bankroll=bankroll_amount,
        unit_size=unit_size,
        kelly_fraction_cap=fractional_kelly,
        max_units=max_bet_units,
    ) if not overview_edges.empty else overview_edges
    overview_tracked = get_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    overview_graded = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    overview_journal = get_journal_entries(sport_label)
    overview_bankroll = build_bankroll_summary(overview_journal, bankroll_amount)
    overview_kpis = build_bankroll_kpis(overview_journal, bankroll_amount)

    overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
    overview_col1.metric("Live Board Rows", f"{len(overview_board)}")
    overview_col2.metric("Live Edge Rows", f"{len(overview_edges)}")
    overview_col3.metric("Tracked Picks", f"{len(overview_tracked)}")
    overview_col4.metric("Graded Picks", f"{len(overview_graded)}")

    overview_col5, overview_col6, overview_col7, overview_col8 = st.columns(4)
    overview_col5.metric("Current Bankroll", f"${overview_bankroll['current_bankroll']}")
    overview_col6.metric("Open Risk", f"${overview_bankroll['open_risk']}")
    overview_col7.metric("ROI", f"{overview_kpis['roi'] * 100:.2f}%")
    overview_col8.metric("Yield", f"{overview_kpis['yield_pct'] * 100:.2f}%")

    left_col, right_col = st.columns(2)
    with left_col:
        st.markdown("### Top Live Edges")
        if overview_edges.empty:
            st.caption("No live edges available yet.")
        else:
            top_overview_edges = overview_edges[overview_edges["coverage_status"] == "Live"].copy() if "coverage_status" in overview_edges.columns else overview_edges.copy()
            top_overview_edges = top_overview_edges.sort_values(["confidence", "edge"], ascending=False).head(10)
            if not top_overview_edges.empty:
                display_overview_edges = top_overview_edges[
                    ["player", "market", "pick", "sportsbook", "edge", "confidence", "recommended_units", "recommended_stake"]
                ].copy()
                display_overview_edges["edge"] = (display_overview_edges["edge"] * 100).round(2)
                st.dataframe(display_overview_edges, use_container_width=True)

    with right_col:
        st.markdown("### Saved Ticket Snapshot")
        overview_tickets = get_ticket_summary_with_grades(sport_label)
        if overview_tickets.empty:
            st.caption("No saved tickets yet.")
        else:
            st.dataframe(
                overview_tickets[
                    ["ticket_id", "name", "source", "leg_count", "avg_confidence", "ticket_status_live", "created_at"]
                ].head(10),
                use_container_width=True,
            )

with tab1:
    render_section_header("Live Board", "Inspect the latest normalized market rows with quick filters and export controls.")
    board = pd.DataFrame()

    if live_sport_keys:
        board = get_latest_board(live_sport_keys, is_dfs=is_dfs)

    if board.empty:
        st.warning("No board data found yet. Run a sync or seed the database before loading the live board.")
    else:
        board = apply_market_coverage(board, market_coverage_map)
        show_non_live_board = st.checkbox(
            "Show demo-only/provider-unavailable markets",
            value=not sync_enabled,
            key="show_non_live_board",
        )
        display_board = board if show_non_live_board else board[board["coverage_status"] == "Live"].copy()
        board_filter_col1, board_filter_col2, board_filter_col3, board_filter_col4 = st.columns(4)
        board_market_filter = board_filter_col1.selectbox(
            "Market filter",
            [""] + sorted(display_board["market"].dropna().astype(str).unique().tolist()) if not display_board.empty else [""],
            key="board_market_filter",
        )
        board_player_filter = board_filter_col2.text_input("Player search", key="board_player_filter")
        board_sort_by = board_filter_col3.selectbox(
            "Sort by",
            [col for col in ["pulled_at", "line", "price", "player", "market"] if col in display_board.columns] if not display_board.empty else [""],
            key="board_sort_by",
        )
        board_sort_ascending = board_filter_col4.checkbox("Ascending", value=False, key="board_sort_ascending")
        display_board = filter_dataframe(
            display_board,
            market_key=board_market_filter,
            player_query=board_player_filter,
            sort_by=board_sort_by,
            ascending=board_sort_ascending,
        )
        if display_board.empty:
            st.info("No live-supported markets are currently available for this board view.")
        else:
            st.dataframe(style_coverage_table(compact_numeric_table(display_board)), use_container_width=True)
            st.download_button(
                "Export Live Board CSV",
                data=display_board.to_csv(index=False),
                file_name=f"{sport_label.lower()}_live_board.csv",
                mime="text/csv",
                use_container_width=True,
            )

with tab2:
    render_section_header("Edge Scanner", "Rank live-supported props by model edge, confidence, and suggested stake size.")
    edge_df = pd.DataFrame()

    if live_sport_keys:
        edge_df = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs)

    if edge_df.empty:
        st.warning("No edge data found. You may need both synced market lines and saved projections first.")
    else:
        edge_df = apply_market_coverage(edge_df, market_coverage_map)
        edge_df = annotate_stake_recommendations(
            edge_df,
            bankroll=bankroll_amount,
            unit_size=unit_size,
            kelly_fraction_cap=fractional_kelly,
            max_units=max_bet_units,
        )
        show_non_live_edges = st.checkbox(
            "Show demo-only/provider-unavailable edge rows",
            value=False,
            key="show_non_live_edges",
        )
        display_edges = edge_df if show_non_live_edges else edge_df[edge_df["coverage_status"] == "Live"].copy()
        edge_filter_col1, edge_filter_col2, edge_filter_col3, edge_filter_col4 = st.columns(4)
        edge_market_filter = edge_filter_col1.selectbox(
            "Market filter",
            [""] + sorted(display_edges["market"].dropna().astype(str).unique().tolist()) if not display_edges.empty else [""],
            key="edge_market_filter",
        )
        edge_player_filter = edge_filter_col2.text_input("Player search", key="edge_player_filter")
        edge_sort_by = edge_filter_col3.selectbox(
            "Sort by",
            [col for col in ["confidence", "edge", "model_prob", "recommended_stake", "player"] if col in display_edges.columns] if not display_edges.empty else [""],
            key="edge_sort_by",
        )
        edge_sort_ascending = edge_filter_col4.checkbox("Ascending", value=False, key="edge_sort_ascending")
        display_edges = filter_dataframe(
            display_edges,
            market_key=edge_market_filter,
            player_query=edge_player_filter,
            sort_by=edge_sort_by,
            ascending=edge_sort_ascending,
        )
        if display_edges.empty:
            st.info("No live-supported edge rows are available for this sport right now.")
        else:
            st.dataframe(style_coverage_table(compact_numeric_table(display_edges)), use_container_width=True)
            st.download_button(
                "Export Edge Scanner CSV",
                data=display_edges.to_csv(index=False),
                file_name=f"{sport_label.lower()}_edge_scanner.csv",
                mime="text/csv",
                use_container_width=True,
            )

        st.markdown("### Best Prop Cards")
        cards = build_prop_cards(display_edges, top_n=10)

        for card in cards:
            render_prop_card(card)

        track_count = st.slider("Track top live edges", min_value=1, max_value=25, value=5, key="track_top_edges")
        if st.button("Save Top Live Edges For Grading", use_container_width=True):
            rows_to_track = display_edges.head(track_count).copy()
            tracked = track_edge_rows(rows_to_track, sport_key=live_sport_keys[0], source="edge_scanner")
            st.success(f"Saved {tracked} live edge rows to the grading tracker.")

with tab3:
    render_section_header("Parlay Lab", "Build live or demo tickets with clearer stake planning and model context.")
    source = st.radio("Parlay Source", ["Live edges", "Demo predictions"], horizontal=True)

    if source == "Live edges":
        edge_df = pd.DataFrame()

        if live_sport_keys:
            edge_df = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs)

        if edge_df.empty:
            st.info("Live edge data is empty, so the parlay builder cannot rank legs yet.")
        else:
            edge_df = apply_market_coverage(edge_df, market_coverage_map)
            edge_df = annotate_stake_recommendations(
                edge_df,
                bankroll=bankroll_amount,
                unit_size=unit_size,
                kelly_fraction_cap=fractional_kelly,
                max_units=max_bet_units,
            )
            legs = st.slider("Legs", min_value=2, max_value=6, value=3)
            min_confidence = st.slider("Minimum confidence", min_value=50, max_value=95, value=65)
            allow_same_player = st.checkbox("Allow multiple picks on the same player", value=False)

            candidates = edge_df.copy()
            candidates = candidates[candidates["coverage_status"] == "Live"].copy()
            candidates = candidates[candidates["confidence"] >= min_confidence].copy()
            candidates = candidates.sort_values(["confidence", "edge"], ascending=False)

            if not allow_same_player:
                candidates = candidates.drop_duplicates(subset=["player"], keep="first")

            parlay_df = candidates.head(legs).copy()
            live_ticket_name = st.text_input("Live ticket name", value=f"{sport_label} Live Ticket", key="live_ticket_name")
            live_ticket_notes = st.text_input("Live ticket notes", key="live_ticket_notes")

            if parlay_df.empty or len(parlay_df) < legs:
                st.warning("Not enough qualifying live legs for the current settings.")
            else:
                st.caption("Live parlay mode only uses markets currently marked `Live` for this provider.")
                parlay_stake_plan = recommend_parlay_stake(
                    parlay_df,
                    bankroll=bankroll_amount,
                    unit_size=unit_size,
                    base_fraction=max(0.03, fractional_kelly * 0.5),
                    max_units=max(1.0, max_bet_units - 0.5),
                )
                stake_col1, stake_col2, stake_col3, stake_col4 = st.columns(4)
                stake_col1.metric("Ticket Stake", f"{parlay_stake_plan['recommended_units']}u")
                stake_col2.metric("Ticket Dollars", f"${parlay_stake_plan['recommended_stake']}")
                stake_col3.metric("Parlay Model Prob", f"{parlay_stake_plan['parlay_model_prob'] * 100:.2f}%")
                stake_col4.metric("Parlay Edge", f"{parlay_stake_plan['parlay_edge'] * 100:.2f}%")
                st.caption(
                    f"Singles equivalent stake: ${parlay_stake_plan['singles_total_stake']} total "
                    f"across an average {parlay_stake_plan['avg_leg_units']}u per leg. "
                    f"Estimated parlay decimal odds: {parlay_stake_plan['parlay_decimal_odds']}."
                )
                parlay_df.insert(0, "leg_rank", range(1, len(parlay_df) + 1))
                st.dataframe(
                    style_coverage_table(compact_numeric_table(parlay_df[
                        [
                            "leg_rank",
                            "player",
                            "market",
                            "pick",
                            "sportsbook",
                            "line",
                            "projection",
                            "model_prob",
                            "implied_prob",
                            "edge",
                            "confidence",
                            "recommended_units",
                            "recommended_stake",
                            "coverage_status",
                        ]
                    ])),
                    use_container_width=True,
                )
        if st.button("Save Live Ticket", use_container_width=True):
                    ticket_id = save_ticket(
                        ticket_name=live_ticket_name,
                        sport_label=sport_label,
                        source="live_edges",
                        legs_df=parlay_df,
                        notes=live_ticket_notes or None,
                    )
                    if ticket_id:
                        st.success(f"Saved live ticket #{ticket_id}.")
                        if st.checkbox("Also add this live ticket to bankroll journal", value=False, key="log_live_ticket_journal"):
                            journal_id = add_journal_entry(
                                entry_type="ticket",
                                label=live_ticket_name,
                                sport_label=sport_label,
                                ticket_id=ticket_id,
                                stake_dollars=parlay_stake_plan["recommended_stake"],
                                stake_units=parlay_stake_plan["recommended_units"],
                                suggested_stake_dollars=parlay_stake_plan["recommended_stake"],
                                suggested_stake_units=parlay_stake_plan["recommended_units"],
                                notes=live_ticket_notes or None,
                            )
                            st.info(f"Logged bankroll journal entry #{journal_id}.")
    else:
        st.caption("Demo mode uses the built-in synthetic prediction engine so you can iterate without live synced data.")
        demo_service = ResearchService()
        demo_bundle = demo_service.build_predictions(sport=sport_config["demo_key"])

        legs = st.slider("Demo legs", min_value=2, max_value=6, value=3, key="demo_legs")
        min_confidence = st.slider("Demo minimum confidence", min_value=50, max_value=95, value=70, key="demo_conf")
        allow_same_team = st.checkbox("Allow same-team demo legs", value=False)
        style = st.selectbox("Parlay style", ["Safe", "Balanced", "Aggressive"])
        demo_ticket_name = st.text_input("Demo ticket name", value=f"{sport_label} Demo Ticket", key="demo_ticket_name")
        demo_ticket_notes = st.text_input("Demo ticket notes", key="demo_ticket_notes")

        parlay = build_parlay(
            demo_bundle.predictions,
            ParlaySettings(
                legs=legs,
                min_confidence=min_confidence,
                allow_same_team=allow_same_team,
                style=style,
            ),
        )

        if parlay.empty:
            st.warning("No demo parlay met the current filters.")
        else:
            st.dataframe(compact_numeric_table(parlay), use_container_width=True)
            if st.button("Save Demo Ticket", use_container_width=True):
                ticket_id = save_ticket(
                    ticket_name=demo_ticket_name,
                    sport_label=sport_label,
                    source="demo_predictions",
                    legs_df=parlay,
                    notes=demo_ticket_notes or None,
                )
                if ticket_id:
                    st.success(f"Saved demo ticket #{ticket_id}.")

with tab4:
    render_section_header("Line History", "Trace market movement across books and pull up sample players faster.")
    if history_suggestions["players"] or history_suggestions["markets"]:
        with st.expander("Suggested seeded players and markets", expanded=False):
            if history_suggestions["players"]:
                st.write("Players:")
                st.code(", ".join(history_suggestions["players"]))
            if history_suggestions["markets"]:
                st.write("Markets:")
                st.code(", ".join(history_suggestions["markets"]))

    quick_fill_col1, quick_fill_col2 = st.columns(2)
    with quick_fill_col1:
        selected_player = st.selectbox(
            "Quick-fill Player",
            [""] + history_suggestions["players"],
            index=0,
            key="history_player_suggestion",
        )
        if st.button("Use Player Suggestion", use_container_width=True):
            st.session_state["history_player_input"] = selected_player

    with quick_fill_col2:
        selected_market = st.selectbox(
            "Quick-fill Market",
            [""] + history_suggestions["markets"],
            index=0,
            key="history_market_suggestion",
        )
        if st.button("Use Market Suggestion", use_container_width=True):
            st.session_state["history_market_input"] = selected_market

    player = st.text_input("Player Name", key="history_player_input")
    market = st.text_input("Market Key (example: player_points)", key="history_market_input")

    if st.button("Load History"):
        history = pd.DataFrame()

        if live_sport_keys:
            history = get_line_history(
                sport_key=live_sport_keys,
                player_name=player if player else None,
                market_key=market if market else None,
            )

        if history.empty:
            st.warning("No history found.")
        else:
            st.dataframe(compact_numeric_table(history), use_container_width=True)

            if "pulled_at" in history.columns and "line" in history.columns:
                chart_df = history.dropna(subset=["pulled_at", "line"]).copy()
                if not chart_df.empty:
                    pivot_df = chart_df.pivot_table(
                        index="pulled_at",
                        columns="book",
                        values="line",
                        aggfunc="last",
                    )
                    st.line_chart(pivot_df)

with tab5:
    render_section_header("Results & Grading", "Resolve props, review bankroll movement, and compare actual outcomes against model expectations.")
    tracked_df = get_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    unresolved_tracked_df = get_unresolved_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    graded_df = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    results_df = get_prop_results(live_sport_keys) if live_sport_keys else pd.DataFrame()
    auto_settle_scope = ",".join(live_sport_keys) if live_sport_keys else ""
    auto_settle_payload = get_sync_payload("sportsgameodds_auto_settle", auto_settle_scope) if auto_settle_scope else {}
    journal_df = get_journal_entries(sport_label)
    bankroll_summary = build_bankroll_summary(journal_df, bankroll_amount)
    bankroll_kpis = build_bankroll_kpis(journal_df, bankroll_amount)

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Tracked Picks", f"{len(tracked_df)}")
    metric_col2.metric("Settled Results", f"{len(results_df)}")
    metric_col3.metric("Graded Picks", f"{len(graded_df)}")
    metric_col4.metric("Open Tracked Picks", f"{len(unresolved_tracked_df)}")

    if auto_settle_payload:
        recorded_at = str(auto_settle_payload.get("recorded_at") or "")
        summary_bits = []
        if recorded_at:
            summary_bits.append(f"Last auto-settle: {recorded_at}")
        if auto_settle_payload.get("rows_imported") is not None:
            summary_bits.append(f"Imported: {auto_settle_payload['rows_imported']}")
        if auto_settle_payload.get("events_fetched") is not None:
            summary_bits.append(f"Finalized events checked: {auto_settle_payload['events_fetched']}")
        if auto_settle_payload.get("matched_results") is not None:
            summary_bits.append(f"Matched tracked picks: {auto_settle_payload['matched_results']}")
        if summary_bits:
            st.caption(" | ".join(summary_bits))

    st.markdown("### Bankroll Journal")
    journal_col1, journal_col2, journal_col3, journal_col4 = st.columns(4)
    journal_col1.metric("Starting Bankroll", f"${bankroll_summary['starting_bankroll']}")
    journal_col2.metric("Open Risk", f"${bankroll_summary['open_risk']}")
    journal_col3.metric("Realized Profit", f"${bankroll_summary['realized_profit']}")
    journal_col4.metric("Current Bankroll", f"${bankroll_summary['current_bankroll']}")

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    kpi_col1.metric("Turnover", f"${bankroll_kpis['turnover']}")
    kpi_col2.metric("ROI", f"{bankroll_kpis['roi'] * 100:.2f}%")
    kpi_col3.metric("Yield", f"{bankroll_kpis['yield_pct'] * 100:.2f}%")
    kpi_col4.metric("Win Rate", f"{bankroll_kpis['win_rate'] * 100:.2f}%")

    kpi_col5, kpi_col6, kpi_col7, kpi_col8 = st.columns(4)
    kpi_col5.metric("Resolved Entries", f"{bankroll_kpis['resolved_entries']}")
    kpi_col6.metric("Open Entries", f"{bankroll_kpis['open_entries']}")
    kpi_col7.metric("Avg Stake", f"${bankroll_kpis['avg_stake']}")
    kpi_col8.metric("Bankroll Change", f"{bankroll_kpis['bankroll_change_pct'] * 100:.2f}%")

    if not journal_df.empty:
        resolved_journal = journal_df[journal_df["resolved_at"].notna()].copy()
        if not resolved_journal.empty:
            resolved_journal = resolved_journal.sort_values("resolved_at")
            resolved_journal["bankroll_after"] = bankroll_amount + resolved_journal["realized_profit_dollars"].fillna(0.0).cumsum()
            bankroll_trend = resolved_journal.set_index("resolved_at")[["bankroll_after"]]
            st.line_chart(bankroll_trend)

    journal_label = st.text_input("Journal label", value=f"{sport_label} manual bet", key="journal_label")
    journal_stake = st.number_input("Actual stake ($)", min_value=1.0, value=float(unit_size), step=1.0, key="journal_stake")
    journal_units = st.number_input("Actual stake (units)", min_value=0.1, value=1.0, step=0.1, key="journal_units")
    journal_notes = st.text_input("Journal notes", key="journal_notes")
    if st.button("Add Manual Journal Entry", use_container_width=True):
        journal_id = add_journal_entry(
            entry_type="manual",
            label=journal_label,
            sport_label=sport_label,
            stake_dollars=journal_stake,
            stake_units=journal_units,
            notes=journal_notes or None,
        )
        st.success(f"Added bankroll journal entry #{journal_id}.")
        st.rerun()

    if journal_df.empty:
        st.caption("No bankroll journal entries yet.")
    else:
        if st.button("Auto-Sync Ticket Journal Entries", use_container_width=True):
            journal_sync_result = sync_ticket_journal_entries(sport_label)
            if journal_sync_result["settled_entries"] > 0:
                st.success(
                    f"Auto-settled {journal_sync_result['settled_entries']} ticket-linked journal entries."
                )
            else:
                st.info("No open ticket-linked journal entries were ready to settle.")
            st.rerun()
        st.dataframe(compact_numeric_table(journal_df), use_container_width=True)
        open_entries = journal_df[journal_df["status"] == "open"].copy()
        if not open_entries.empty:
            selected_journal_id = st.selectbox(
                "Settle journal entry",
                open_entries["journal_entry_id"].tolist(),
                key="selected_journal_id",
            )
            realized_profit = st.number_input(
                "Realized profit/loss ($)",
                value=0.0,
                step=1.0,
                key="journal_realized_profit",
            )
            journal_status = st.selectbox(
                "Journal status",
                ["won", "lost", "push", "cancelled"],
                key="journal_status",
            )
            if st.button("Settle Journal Entry", use_container_width=True):
                settle_journal_entry(int(selected_journal_id), realized_profit_dollars=float(realized_profit), status=journal_status)
                st.success("Updated bankroll journal entry.")
                st.rerun()

    ungraded_df = unresolved_tracked_df.copy()

    auto_settle_days = st.slider(
        "Auto-settle finalized lookback (days)",
        min_value=3,
        max_value=21,
        value=7,
        key="auto_settle_days",
    )
    if st.button("Auto-Sync Settled Results From SportsGameOdds", use_container_width=True):
        try:
            settle_result = sync_prop_results_from_sportsgameodds(live_sport_keys, days=auto_settle_days)
            journal_sync_result = sync_ticket_journal_entries(sport_label)
            st.success(
                f"Imported {settle_result['rows_imported']} settled results from "
                f"{settle_result['events_fetched']} finalized SportsGameOdds events."
            )
            if journal_sync_result["settled_entries"] > 0:
                st.info(
                    f"Also auto-settled {journal_sync_result['settled_entries']} ticket-linked bankroll entries "
                    f"({journal_sync_result['won_entries']} won, {journal_sync_result['lost_entries']} lost, "
                    f"{journal_sync_result['push_entries']} push)."
                )
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    selection_options: list[str] = []
    selection_map: dict[str, dict] = {}
    if not ungraded_df.empty:
        for _, row in ungraded_df.head(100).iterrows():
            label = (
                f"{row['player']} | {row['market']} | {row['pick']} | "
                f"{row['sportsbook']} | line {row['line']} | {row['event_id']}"
            )
            selection_options.append(label)
            selection_map[label] = row.to_dict()

    st.markdown("### Enter Settled Result")
    selected_pick_label = st.selectbox(
        "Tracked pick",
        [""] + selection_options,
        key="result_pick_selector",
    )
    actual_value = st.number_input("Actual stat value", value=0.0, step=0.5, key="result_actual_value")
    use_actual_value = st.checkbox("Use numeric result", value=True, key="use_actual_result")
    winning_side = st.selectbox("Winning side (for yes/no markets)", ["", "yes", "no", "over", "under"], key="result_winning_side")
    result_notes = st.text_input("Result notes", key="result_notes")

    if st.button("Save Settled Result", use_container_width=True):
        selected = selection_map.get(selected_pick_label)
        if not selected:
            st.warning("Choose a tracked pick first.")
        elif not use_actual_value and not winning_side:
            st.warning("Enter either a numeric result or a winning side.")
        else:
            upsert_prop_result(
                sport_key=str(selected["sport_key"]),
                event_id=str(selected["event_id"]),
                market_key=str(selected["market"]),
                player_name=str(selected["player"]),
                actual_value=float(actual_value) if use_actual_value else None,
                winning_side=winning_side or None,
                source="manual_entry",
                notes=result_notes or None,
            )
            st.success("Saved settled result.")

    st.markdown("### Ungraded Tracked Picks")
    if ungraded_df.empty:
        st.caption("No ungraded tracked picks yet.")
    else:
        st.dataframe(compact_numeric_table(ungraded_df.head(100)), use_container_width=True)
        st.download_button(
            "Export Ungraded Tracked Picks CSV",
            data=ungraded_df.to_csv(index=False),
            file_name=f"{sport_label.lower()}_tracked_picks.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown("### Graded Picks")
    if graded_df.empty:
        st.caption("No graded picks yet. Track live edges, then enter settled results here.")
    else:
        graded_filter_col1, graded_filter_col2, graded_filter_col3 = st.columns(3)
        graded_market_filter = graded_filter_col1.selectbox(
            "Graded market filter",
            [""] + sorted(graded_df["market"].dropna().astype(str).unique().tolist()),
            key="graded_market_filter",
        )
        graded_player_filter = graded_filter_col2.text_input("Graded player search", key="graded_player_filter")
        graded_sort_by = graded_filter_col3.selectbox(
            "Graded sort by",
            [col for col in ["resolved_at", "profit_units", "edge", "model_prob", "confidence"] if col in graded_df.columns],
            key="graded_sort_by",
        )
        graded_df = filter_dataframe(
            graded_df,
            market_key=graded_market_filter,
            player_query=graded_player_filter,
            sort_by=graded_sort_by,
            ascending=False,
        )
        graded_display = graded_df[
            [
                "player",
                "market",
                "pick",
                "sportsbook",
                "line",
                "actual_value",
                "winning_side",
                "grade",
                "profit_units",
                "model_prob",
                "edge",
                "tracked_at",
                "resolved_at",
            ]
        ].copy()
        graded_display["model_prob"] = (graded_display["model_prob"] * 100).round(2)
        graded_display["edge"] = (graded_display["edge"] * 100).round(2)
        graded_display["profit_units"] = graded_display["profit_units"].round(3)
        st.dataframe(compact_numeric_table(graded_display.head(200)), use_container_width=True)
        st.download_button(
            "Export Graded Picks CSV",
            data=graded_df.to_csv(index=False),
            file_name=f"{sport_label.lower()}_graded_picks.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown("### Import / Export Results")
    if not results_df.empty:
        st.download_button(
            "Export Settled Results CSV",
            data=results_df.to_csv(index=False),
            file_name=f"{sport_label.lower()}_settled_results.csv",
            mime="text/csv",
            use_container_width=True,
        )

    results_upload = st.file_uploader(
        "Import settled results CSV",
        type=["csv"],
        key="results_csv_upload",
    )
    if results_upload is not None and st.button("Import Settled Results CSV", use_container_width=True):
        try:
            result = import_prop_results_csv(results_upload.getvalue())
            st.success(f"Imported {result['rows_imported']} settled result rows.")
        except Exception as exc:
            st.error(str(exc))

    tracked_upload = st.file_uploader(
        "Import tracked picks CSV",
        type=["csv"],
        key="tracked_csv_upload",
    )
    if tracked_upload is not None and st.button("Import Tracked Picks CSV", use_container_width=True):
        try:
            result = import_tracked_picks_csv(tracked_upload.getvalue())
            st.success(f"Imported {result['rows_imported']} tracked pick rows.")
        except Exception as exc:
            st.error(str(exc))

    st.markdown("### Saved Tickets")
    ticket_summary_df = get_ticket_summary_with_grades(sport_label)
    if ticket_summary_df.empty:
        st.caption("No saved tickets yet. Save one from Parlay Lab to start tracking tickets.")
    else:
        display_tickets = ticket_summary_df[
            [
                "ticket_id",
                "name",
                "source",
                "leg_count",
                "avg_confidence",
                "avg_model_prob",
                "resolved_legs",
                "won_legs",
                "push_legs",
                "open_legs",
                "ticket_status_live",
                "created_at",
            ]
        ].copy()
        display_tickets["avg_confidence"] = display_tickets["avg_confidence"].round(1)
        display_tickets["avg_model_prob"] = (display_tickets["avg_model_prob"] * 100).round(2)
        st.dataframe(compact_numeric_table(display_tickets), use_container_width=True)
        ticket_export_df = export_ticket_legs_for_csv(sport_label)
        if not ticket_export_df.empty:
            st.download_button(
                "Export Saved Tickets CSV",
                data=ticket_export_df.to_csv(index=False),
                file_name=f"{sport_label.lower()}_saved_tickets.csv",
                mime="text/csv",
                use_container_width=True,
            )

        ticket_upload = st.file_uploader(
            "Import saved tickets CSV",
            type=["csv"],
            key="tickets_csv_upload",
        )
        if ticket_upload is not None and st.button("Import Saved Tickets CSV", use_container_width=True):
            try:
                result = import_ticket_legs_csv(ticket_upload.getvalue())
                st.success(f"Imported {result['tickets_created']} saved tickets.")
            except Exception as exc:
                st.error(str(exc))

        selected_ticket_id = st.selectbox(
            "Inspect saved ticket",
            display_tickets["ticket_id"].tolist(),
            key="selected_ticket_id",
        )
        selected_legs = get_ticket_legs(int(selected_ticket_id))
        if not selected_legs.empty:
            st.dataframe(compact_numeric_table(selected_legs), use_container_width=True)

            selected_ticket_meta = ticket_summary_df[ticket_summary_df["ticket_id"] == selected_ticket_id].head(1)
            if not selected_ticket_meta.empty:
                ticket_row = selected_ticket_meta.iloc[0]
                st.markdown("#### Ticket vs Model Comparison")

                if ticket_row["source"] == "live_edges":
                    ticket_legs_with_results = get_ticket_legs_with_results(int(selected_ticket_id), sport_label)
                    graded_pool = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
                    benchmark = build_ticket_benchmark_summary(graded_pool, int(ticket_row["leg_count"]))

                    current_live_edges = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs) if live_sport_keys else pd.DataFrame()
                    current_live_edges = apply_market_coverage(current_live_edges, market_coverage_map)
                    current_live_edges = current_live_edges[current_live_edges["coverage_status"] == "Live"].copy() if not current_live_edges.empty else current_live_edges
                    current_benchmark = current_live_edges.sort_values(["confidence", "edge"], ascending=False).head(int(ticket_row["leg_count"])) if not current_live_edges.empty else pd.DataFrame()

                    overlap_count = 0
                    if not current_benchmark.empty:
                        ticket_keys = {
                            (str(row["player"]), str(row["market"]), str(row["pick"]))
                            for _, row in selected_legs.iterrows()
                        }
                        benchmark_keys = {
                            (str(row["player"]), str(row["market"]), str(row["pick"]))
                            for _, row in current_benchmark.iterrows()
                        }
                        overlap_count = len(ticket_keys.intersection(benchmark_keys))

                    comparison_col1, comparison_col2, comparison_col3, comparison_col4 = st.columns(4)
                    comparison_col1.metric("Ticket Avg Confidence", f"{float(ticket_row['avg_confidence'] or 0):.1f}")
                    comparison_col2.metric("Ticket Avg Model Prob", f"{float((ticket_row['avg_model_prob'] or 0) * 100):.2f}%")
                    comparison_col3.metric("Benchmark Avg Confidence", f"{float(benchmark['benchmark_avg_confidence'] or 0):.1f}")
                    comparison_col4.metric("Current Top-Leg Overlap", f"{overlap_count}/{int(ticket_row['leg_count'])}")

                    comparison_col5, comparison_col6, comparison_col7 = st.columns(3)
                    comparison_col5.metric(
                        "Benchmark Hit Rate",
                        f"{float((benchmark['benchmark_hit_rate'] or 0) * 100):.2f}%" if benchmark["benchmark_hit_rate"] is not None else "N/A",
                    )
                    comparison_col6.metric(
                        "Benchmark Profit Units",
                        f"{float(benchmark['benchmark_profit_units'] or 0):.2f}" if benchmark["benchmark_profit_units"] is not None else "N/A",
                    )
                    comparison_col7.metric("Ticket Status", str(ticket_row["ticket_status_live"]))

                    if not ticket_legs_with_results.empty:
                        st.markdown("##### Ticket Leg Outcomes")
                        st.dataframe(
                            compact_numeric_table(
                            ticket_legs_with_results[
                                [
                                    "leg_rank",
                                    "player",
                                    "market",
                                    "pick",
                                    "line",
                                    "actual_value",
                                    "winning_side",
                                    "grade",
                                    "sportsbook",
                                ]
                            ]),
                            use_container_width=True,
                        )

                    if not current_benchmark.empty:
                        st.markdown("##### Current Top Model Legs")
                        benchmark_display = current_benchmark[
                            [
                                "player",
                                "market",
                                "pick",
                                "sportsbook",
                                "line",
                                "model_prob",
                                "edge",
                                "confidence",
                                "recommended_units",
                                "recommended_stake",
                            ]
                        ].copy()
                        benchmark_display["model_prob"] = (benchmark_display["model_prob"] * 100).round(2)
                        benchmark_display["edge"] = (benchmark_display["edge"] * 100).round(2)
                        st.dataframe(compact_numeric_table(benchmark_display), use_container_width=True)

                    saved_ticket_stake_plan = recommend_parlay_stake(
                        selected_legs,
                        bankroll=bankroll_amount,
                        unit_size=unit_size,
                        base_fraction=max(0.03, fractional_kelly * 0.5),
                        max_units=max(1.0, max_bet_units - 0.5),
                    )
                    st.markdown("##### Ticket Stake Plan")
                    saved_stake_col1, saved_stake_col2, saved_stake_col3, saved_stake_col4 = st.columns(4)
                    saved_stake_col1.metric("Suggested Ticket Stake", f"{saved_ticket_stake_plan['recommended_units']}u")
                    saved_stake_col2.metric("Suggested Dollars", f"${saved_ticket_stake_plan['recommended_stake']}")
                    saved_stake_col3.metric("Parlay Model Prob", f"{saved_ticket_stake_plan['parlay_model_prob'] * 100:.2f}%")
                    saved_stake_col4.metric("Est. Decimal Odds", f"{saved_ticket_stake_plan['parlay_decimal_odds']}")
                    st.caption(
                        f"Singles equivalent stake: ${saved_ticket_stake_plan['singles_total_stake']} total. "
                        f"Parlay edge estimate: {saved_ticket_stake_plan['parlay_edge'] * 100:.2f}%."
                    )
                else:
                    st.caption("Comparison benchmarks are currently available for live saved tickets. Demo tickets are stored for workflow tracking but do not grade against live results.")

with tab6:
    render_section_header("Backtest", "Review true-results performance, calibration, CLV proxy signals, and profit trends.")
    true_backtest_df = pd.DataFrame()
    clv_backtest_df = pd.DataFrame()

    if live_sport_keys:
        true_backtest_df = build_true_backtest(live_sport_keys)
        clv_backtest_df = build_clv_backtest(live_sport_keys)

    if true_backtest_df.empty and clv_backtest_df.empty:
        st.info("Not enough live history or graded results yet to build diagnostics for this sport.")
    else:
        if not true_backtest_df.empty:
            st.markdown("### True Results Backtest")
            total_picks = len(true_backtest_df)
            hit_rate = true_backtest_df["won"].mean()
            avg_edge = true_backtest_df["edge"].mean()
            total_profit_units = true_backtest_df["profit_units"].sum()

            metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
            metric_col1.metric("Graded Picks", f"{total_picks}")
            metric_col2.metric("Hit Rate", f"{hit_rate:.1%}")
            metric_col3.metric("Avg Edge", f"{avg_edge:.1%}")
            metric_col4.metric("Profit Units", f"{total_profit_units:.2f}")

            calibration_df = build_true_calibration_summary(true_backtest_df)
            if not calibration_df.empty:
                st.markdown("#### Probability Buckets")
                display_calibration = calibration_df.copy()
                display_calibration["avg_model_prob"] = (display_calibration["avg_model_prob"] * 100).round(2)
                display_calibration["hit_rate"] = (display_calibration["hit_rate"] * 100).round(2)
                display_calibration["avg_edge"] = (display_calibration["avg_edge"] * 100).round(2)
                display_calibration["profit_units"] = display_calibration["profit_units"].round(3)
                st.dataframe(compact_numeric_table(display_calibration), use_container_width=True)

                calibration_chart = calibration_df.copy()
                calibration_chart["prob_bucket"] = calibration_chart["prob_bucket"].astype(str)
                calibration_chart = calibration_chart.set_index("prob_bucket")[["avg_model_prob", "hit_rate"]]
                st.line_chart(calibration_chart)

            market_summary_df = build_true_market_summary(true_backtest_df)
            if not market_summary_df.empty:
                st.markdown("#### Market Breakdown")
                display_market_summary = market_summary_df.copy()
                display_market_summary["hit_rate"] = (display_market_summary["hit_rate"] * 100).round(2)
                display_market_summary["avg_model_prob"] = (display_market_summary["avg_model_prob"] * 100).round(2)
                display_market_summary["avg_edge"] = (display_market_summary["avg_edge"] * 100).round(2)
                display_market_summary["profit_units"] = display_market_summary["profit_units"].round(3)
                st.dataframe(compact_numeric_table(display_market_summary), use_container_width=True)

            sportsbook_summary_df = build_true_sportsbook_summary(true_backtest_df)
            if not sportsbook_summary_df.empty:
                st.markdown("#### Sportsbook Breakdown")
                display_sportsbook_summary = sportsbook_summary_df.copy()
                display_sportsbook_summary["hit_rate"] = (display_sportsbook_summary["hit_rate"] * 100).round(2)
                display_sportsbook_summary["avg_edge"] = (display_sportsbook_summary["avg_edge"] * 100).round(2)
                display_sportsbook_summary["profit_units"] = display_sportsbook_summary["profit_units"].round(3)
                st.dataframe(compact_numeric_table(display_sportsbook_summary), use_container_width=True)

            confidence_summary_df = build_true_confidence_summary(true_backtest_df)
            if not confidence_summary_df.empty:
                st.markdown("#### Confidence Breakdown")
                display_confidence_summary = confidence_summary_df.copy()
                display_confidence_summary["hit_rate"] = (display_confidence_summary["hit_rate"] * 100).round(2)
                display_confidence_summary["avg_edge"] = (display_confidence_summary["avg_edge"] * 100).round(2)
                display_confidence_summary["profit_units"] = display_confidence_summary["profit_units"].round(3)
                st.dataframe(compact_numeric_table(display_confidence_summary), use_container_width=True)

                confidence_chart = confidence_summary_df.copy()
                confidence_chart["confidence_bucket"] = confidence_chart["confidence_bucket"].astype(str)
                confidence_chart = confidence_chart.set_index("confidence_bucket")[["hit_rate", "profit_units"]]
                st.bar_chart(confidence_chart)

            true_display = true_backtest_df[
                [
                    "player",
                    "market",
                    "sportsbook",
                    "pick",
                    "line",
                    "actual_value",
                    "winning_side",
                    "grade",
                    "profit_units",
                    "model_prob",
                    "edge",
                ]
            ].copy()
            true_display["model_prob"] = (true_display["model_prob"] * 100).round(2)
            true_display["edge"] = (true_display["edge"] * 100).round(2)
            true_display["profit_units"] = true_display["profit_units"].round(3)
            st.dataframe(compact_numeric_table(true_display.head(200)), use_container_width=True)

        if not clv_backtest_df.empty:
            st.markdown("### CLV Proxy Diagnostics")
            clv_hit_rate = clv_backtest_df["clv_win"].mean()
            avg_line_move = clv_backtest_df["line_move"].mean()
            avg_edge = clv_backtest_df["edge"].mean()

            metric_col1, metric_col2, metric_col3 = st.columns(3)
            metric_col1.metric("Tracked CLV Rows", f"{len(clv_backtest_df)}")
            metric_col2.metric("CLV Hit Rate", f"{clv_hit_rate:.1%}")
            metric_col3.metric("Avg Line Move", f"{avg_line_move:.3f}")

            calibration_df = build_calibration_summary(clv_backtest_df)
            if not calibration_df.empty:
                display_calibration = calibration_df.copy()
                display_calibration["avg_model_prob"] = (display_calibration["avg_model_prob"] * 100).round(2)
                display_calibration["clv_hit_rate"] = (display_calibration["clv_hit_rate"] * 100).round(2)
                display_calibration["avg_edge"] = (display_calibration["avg_edge"] * 100).round(2)
                display_calibration["avg_line_move"] = display_calibration["avg_line_move"].round(3)
                st.dataframe(compact_numeric_table(display_calibration), use_container_width=True)

with tab7:
    render_section_header("Stats Import", "Feed the hybrid model with provider-derived history, optional APIs, or your own CSV snapshots.")
    st.caption("Upload player stat snapshots so the hybrid projection model can use a non-odds anchor.")

    st.markdown("### API Sync")
    api_days = st.slider(
        "Recent game window (days)",
        min_value=3,
        max_value=30,
        value=14,
        key="api_stats_days",
    )
    current_board = get_latest_board(live_sport_keys, is_dfs=False) if live_sport_keys else pd.DataFrame()
    target_players = (
        sorted(current_board["player"].dropna().unique().tolist())
        if not current_board.empty and "player" in current_board.columns
        else []
    )

    st.caption("Primary path: derive rolling player stats from finalized SportsGameOdds events already tied to your live provider.")
    if st.button("Build Stats From SportsGameOdds History", use_container_width=True):
        try:
            if sport_label not in {"NBA", "MLB", "NFL"}:
                st.warning("SportsGameOdds history sync is currently wired for NBA, MLB, and NFL.")
            elif not target_players:
                st.warning("No live board players found for this sport yet. Run a live sync first, then retry.")
            else:
                sync_result = sync_stats_from_sportsgameodds(
                    sport_label=sport_label,
                    sport_key=live_sport_keys[0],
                    player_names=target_players,
                    days=api_days,
                )
                st.success(
                    f"Built {sync_result['rows_imported']} stat snapshots from "
                    f"{sync_result['events_fetched']} finalized SportsGameOdds events "
                    f"and {sync_result['stat_records_built']} player-market results."
                )
        except Exception as exc:
            st.error(str(exc))
            st.info(
                "SportsGameOdds history sync is unavailable right now. You can still use the CSV template below, "
                "import manual stats, and rebuild projections from the same hybrid model."
            )

    st.markdown("### Optional Secondary Provider")
    if CONFIG.balldontlie_api_key.strip():
        st.caption("BALLDONTLIE is optional here. Use it only if your account plan supports the stats endpoint.")
        if st.button("Pull Recent Stats From BALLDONTLIE", use_container_width=True):
            try:
                if sport_label not in {"NBA", "MLB"}:
                    st.warning("BALLDONTLIE sync is currently wired for NBA and MLB.")
                elif not target_players:
                    st.warning("No live board players found for this sport yet. Run a sync first, then retry.")
                else:
                    sync_result = sync_stats_from_balldontlie(
                        sport_label=sport_label,
                        sport_key=live_sport_keys[0],
                        player_names=target_players,
                        days=api_days,
                    )
                    st.success(
                        f"Pulled BALLDONTLIE stats: {sync_result['rows_imported']} rows from "
                        f"{sync_result['games_fetched']} games and {sync_result['stats_fetched']} stat records."
                    )
            except Exception as exc:
                st.error(str(exc))
                st.info(
                    "BALLDONTLIE sync is unavailable right now. Keep using SportsGameOdds history above or the CSV template "
                    "below, then rebuild live projections."
                )
    else:
        st.caption("No BALLDONTLIE key detected. That is fine if you are using SportsGameOdds history or CSV import.")

    st.markdown("### Fallback Option")
    st.caption(
        "If API stats are unavailable, use the CSV template below as a manual fallback. "
        "Imported CSV stats feed the same hybrid projection model."
    )

    template_df = build_stats_template()
    st.markdown("### CSV Template")
    st.dataframe(compact_numeric_table(template_df), use_container_width=True)
    st.download_button(
        "Download Stats Template CSV",
        data=template_df.to_csv(index=False),
        file_name="player_stats_template.csv",
        mime="text/csv",
        use_container_width=True,
    )

    uploaded_stats = st.file_uploader(
        "Upload stats CSV",
        type=["csv"],
        key="stats_csv_upload",
        help="Required columns: sport_key, player_name, market_key. Optional: season_average, recent_average, last_5_average, trend, sample_size, source.",
    )

    if uploaded_stats is not None and st.button("Import Stats CSV", use_container_width=True):
        try:
            import_result = import_stats_csv(uploaded_stats.getvalue())
            st.success(f"Imported {import_result['rows_imported']} stat rows.")
        except Exception as exc:
            st.error(str(exc))

    imported_stats_df = get_latest_stats_snapshots(live_sport_keys) if live_sport_keys else pd.DataFrame()
    st.markdown("### Current Imported Stats")
    if imported_stats_df.empty:
        st.caption("No imported stats found yet for the selected sport.")
    else:
        st.dataframe(compact_numeric_table(imported_stats_df), use_container_width=True)
        st.download_button(
            "Export Imported Stats CSV",
            data=imported_stats_df.to_csv(index=False),
            file_name=f"{sport_label.lower()}_imported_stats.csv",
            mime="text/csv",
            use_container_width=True,
        )

    if st.button("Rebuild Live Projections From Imported Stats", use_container_width=True):
        rebuilt = build_live_projections_for_sports([sport_label])
        st.success(f"Rebuilt {rebuilt.get(sport_label, 0)} projections with the stats-enhanced hybrid model.")
