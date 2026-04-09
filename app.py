import pandas as pd
import streamlit as st
import json
import base64
from pathlib import Path

from builders.parlays import ParlaySettings, build_parlay
from config import CONFIG
from db import init_db
from ingestion.providers import get_provider
from sports_config import get_market_coverage, get_market_coverage_map, get_sport_config, get_sport_labels, get_sport_provider_name, is_live_sync_enabled, resolve_live_keys_for_label
from services.demo_seed import clear_demo_live_data, seed_all_demo_live_data, seed_demo_live_data
from services.dfs_slip_service import (
    build_dfs_slip_payload,
    format_dfs_slip_json,
    format_dfs_slip_payload,
    format_dfs_slip_text,
    get_dfs_adapter_by_key,
    get_dfs_slip_adapters,
    recommend_dfs_slip_adapter,
)
try:
    from services.analytics import (
        build_calibration_summary,
        build_clv_backtest,
        build_coach_mode_summary,
        build_experiment_snapshot,
        build_model_recommendation_cards,
        build_monthly_model_review,
        build_review_action_checklist,
        build_ticket_benchmark_summary,
        build_ticket_review_insights,
        build_true_backtest,
        build_true_calibration_summary,
        build_true_confidence_summary,
        build_true_market_summary,
        build_true_source_summary,
        build_true_source_timeseries,
        build_true_sportsbook_summary,
        build_weekly_model_review,
    )
except ImportError:
    def build_calibration_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame()

    def build_clv_backtest(sport_keys: list[str] | str) -> pd.DataFrame:
        return pd.DataFrame()

    def build_experiment_snapshot(
        graded_df: pd.DataFrame,
        source_summary_df: pd.DataFrame,
        rolling_window: int = 10,
    ) -> dict[str, object]:
        return {
            "graded_pick_count": int(len(graded_df)) if not graded_df.empty else 0,
            "source_summary": [],
            "recent_experiments": [],
            "cumulative_units": [],
            "rolling_hit_rate": [],
        }

    def build_ticket_benchmark_summary(graded_picks_df: pd.DataFrame, leg_count: int) -> dict[str, float | int | None]:
        return {
            "benchmark_hit_rate": None,
            "benchmark_profit_units": None,
            "benchmark_avg_confidence": None,
            "benchmark_avg_model_prob": None,
            "benchmark_sample_size": 0,
            "leg_count": int(leg_count or 0),
        }

    def build_ticket_review_insights(
        ticket_row: dict[str, object] | pd.Series,
        benchmark: dict[str, object],
        overlap_count: int,
        current_benchmark: pd.DataFrame,
    ) -> list[dict[str, str]]:
        return []

    def build_true_backtest(sport_keys: list[str] | str) -> pd.DataFrame:
        return pd.DataFrame()

    def build_true_calibration_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame()

    def build_true_confidence_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame()

    def build_true_market_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame()

    def build_true_source_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame()

    def build_true_source_timeseries(
        backtest_df: pd.DataFrame,
        rolling_window: int = 10,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        return pd.DataFrame(), pd.DataFrame()

    def build_true_sportsbook_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame()

    def build_weekly_model_review(graded_df: pd.DataFrame) -> dict[str, object]:
        return {
            "current_window_label": "Last 7 days",
            "prior_window_label": "Prior 7 days",
            "current_summary": {},
            "prior_summary": {},
            "source_breakdown": pd.DataFrame(),
            "market_breakdown": pd.DataFrame(),
            "insights": [],
        }

    def build_monthly_model_review(graded_df: pd.DataFrame) -> dict[str, object]:
        return {
            "current_window_label": "Last 30 days",
            "prior_window_label": "Prior 30 days",
            "current_summary": {},
            "prior_summary": {},
            "source_breakdown": pd.DataFrame(),
            "market_breakdown": pd.DataFrame(),
            "insights": [],
        }

    def build_model_recommendation_cards(weekly_review: dict[str, object], monthly_review: dict[str, object], sport_label: str = "") -> list[dict[str, str]]:
        return []

    def build_coach_mode_summary(weekly_review: dict[str, object], monthly_review: dict[str, object], sport_label: str = "") -> str:
        sport_text = f" for {sport_label}" if str(sport_label).strip() else ""
        return f"Coach Mode: weekly and monthly review helpers are still loading{sport_text}, so the app is using its standard workflow posture for now."

    def build_review_action_checklist(weekly_review: dict[str, object], monthly_review: dict[str, object]) -> list[dict[str, object]]:
        return []
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
from services.smart_parlay_profile_service import build_smart_parlay_profiles
from services.smart_pick_service import apply_smart_weight_overrides, build_smart_history_comparison, build_smart_learning_tables, build_smart_pick_audit, build_smart_weight_profile, score_smart_picks
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
from services.notification_state_service import dismiss_notification, get_notification_history_rows, is_notification_visible, reset_notification, snooze_notification
from services.view_preferences_service import get_view_preference, reset_view_preferences, save_view_preference
from services.watchlist_service import (
    add_watchlist_rows,
    annotate_watchlist_movement,
    get_watchlist_alert_settings,
    get_watchlist_alerts,
    get_watchlist_df,
    remove_watchlist_keys,
    save_watchlist_alert_settings,
)

NBA_EXOTIC_DEBUG_PATH = "data/sportsgameodds_nba_exotics_debug.json"
BRANDMARK_PATH = Path("assets/brandmark.png")
BRANDMARK_SVG_PATH = Path("assets/brandmark.svg")


def _build_brandmark_data_uri() -> str:
    if not BRANDMARK_SVG_PATH.exists():
        return ""
    encoded = base64.b64encode(BRANDMARK_SVG_PATH.read_bytes()).decode("utf-8")
    return f"data:image/svg+xml;base64,{encoded}"


BRANDMARK_DATA_URI = _build_brandmark_data_uri()


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
    badge_styles = (
        {
            "Live": ("#8ee3b7", "#0f2b1f"),
            "Demo Only": ("#f5d27a", "#33270f"),
            "Provider Unavailable": ("#ff9d9d", "#33161a"),
        }
        if theme_mode == "Dark"
        else {
            "Live": ("#0f5132", "#d1e7dd"),
            "Demo Only": ("#664d03", "#fff3cd"),
            "Provider Unavailable": ("#842029", "#f8d7da"),
        }
    )
    text_color, bg_color = badge_styles.get(
        status,
        ("#dbe4f0", "#1f2937") if theme_mode == "Dark" else ("#1f2937", "#e5e7eb"),
    )
    return (
        "<span style="
        f"'display:inline-block;padding:0.2rem 0.55rem;border-radius:999px;"
        f"font-size:0.78rem;font-weight:600;background:{bg_color};color:{text_color};'"
        f">{status}</span>"
    )


def style_coverage_table(df: pd.DataFrame):
    if df.empty or "coverage_status" not in df.columns:
        return df

    style_map = (
        {
            "Live": "background-color: #0f2b1f; color: #8ee3b7; font-weight: 600;",
            "Demo Only": "background-color: #33270f; color: #f5d27a; font-weight: 600;",
            "Provider Unavailable": "background-color: #33161a; color: #ff9d9d; font-weight: 600;",
        }
        if theme_mode == "Dark"
        else {
            "Live": "background-color: #d1e7dd; color: #0f5132; font-weight: 600;",
            "Demo Only": "background-color: #fff3cd; color: #664d03; font-weight: 600;",
            "Provider Unavailable": "background-color: #f8d7da; color: #842029; font-weight: 600;",
        }
    )

    def coverage_style(value):
        return style_map.get(str(value), "")

    return df.style.map(coverage_style, subset=["coverage_status"])


def style_signal_table(df: pd.DataFrame):
    if df.empty:
        return df

    styler = df.style

    if "coverage_status" in df.columns:
        coverage_style_map = (
            {
                "Live": "background-color: #0f2b1f; color: #8ee3b7; font-weight: 600;",
                "Demo Only": "background-color: #33270f; color: #f5d27a; font-weight: 600;",
                "Provider Unavailable": "background-color: #33161a; color: #ff9d9d; font-weight: 600;",
            }
            if theme_mode == "Dark"
            else {
                "Live": "background-color: #d1e7dd; color: #0f5132; font-weight: 600;",
                "Demo Only": "background-color: #fff3cd; color: #664d03; font-weight: 600;",
                "Provider Unavailable": "background-color: #f8d7da; color: #842029; font-weight: 600;",
            }
        )

        def coverage_style(value):
            return coverage_style_map.get(str(value), "")

        styler = styler.map(coverage_style, subset=["coverage_status"])

    movement_columns = [col for col in ["line_move_label", "price_move_label"] if col in df.columns]
    if movement_columns:
        movement_style_map = (
            {
                "better": "background-color: #0f2b1f; color: #8ee3b7; font-weight: 700;",
                "worse": "background-color: #33161a; color: #ff9d9d; font-weight: 700;",
                "neutral": "background-color: #1f2937; color: #dbe4f0; font-weight: 700;",
            }
            if theme_mode == "Dark"
            else {
                "better": "background-color: #d1fae5; color: #065f46; font-weight: 700;",
                "worse": "background-color: #fee2e2; color: #991b1b; font-weight: 700;",
                "neutral": "background-color: #e5e7eb; color: #374151; font-weight: 700;",
            }
        )

        def movement_style(value):
            return movement_style_map.get(str(value), "")

        styler = styler.map(movement_style, subset=movement_columns)

    return styler


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
    hero_markup = (
        f'<div class="app-hero">'
        f'<div class="app-hero__brand">'
        f'<img class="app-hero__brandmark" src="{BRANDMARK_DATA_URI}" alt="AI Parlay Builder brandmark" />'
        f'<div>'
        f'<div class="app-hero__eyebrow">AI Parlay Builder</div>'
        f'<div class="app-hero__title">Sharper prop workflows for {sport_label}</div>'
        f'<div class="app-hero__subtitle">Live odds, projection building, grading, bankroll tracking, and ticket planning in one workspace.</div>'
        f'</div>'
        f'</div>'
        f'<div class="app-hero__meta">'
        f'<span class="hero-pill">Provider: {provider}</span>'
        f'<span class="hero-pill">Board: {board_type}</span>'
        f'<span class="hero-pill">{sync_text}</span>'
        f'<span class="hero-pill">Last sync: {last_sync_text}</span>'
        f'</div>'
        f'</div>'
    )
    st.markdown(hero_markup, unsafe_allow_html=True)


def render_section_header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="section-header"><div class="section-header__title">{title}</div><div class="section-header__subtitle">{subtitle}</div></div>',
        unsafe_allow_html=True,
    )


def render_empty_state(title: str, body: str, tone: str = "neutral") -> None:
    tone_map = (
        {
            "neutral": ("#dbe4f0", "#111827", "#334155"),
            "info": ("#93c5fd", "#0f172a", "#1d4ed8"),
            "warning": ("#fcd34d", "#1f2937", "#92400e"),
        }
        if theme_mode == "Dark"
        else {
            "neutral": ("#334155", "#f8fafc", "#e2e8f0"),
            "info": ("#1d4ed8", "#eff6ff", "#bfdbfe"),
            "warning": ("#92400e", "#fffbeb", "#fde68a"),
        }
    )
    text_color, bg_color, border_color = tone_map.get(tone, tone_map["neutral"])
    empty_markup = (
        f'<div style="background:{bg_color};border:1px solid {border_color};border-radius:18px;padding:1rem 1.05rem;margin:0.2rem 0 0.8rem;">'
        f'<div style="font-size:0.98rem;font-weight:800;color:{text_color};margin-bottom:0.2rem;">{title}</div>'
        f'<div style="font-size:0.92rem;color:{text_color};opacity:0.92;line-height:1.5;">{body}</div>'
        f'</div>'
    )
    st.markdown(empty_markup, unsafe_allow_html=True)


def render_workflow_check_item(title: str, ok: bool, detail: str) -> None:
    if theme_mode == "Dark":
        badge_bg = "#0f2b1f" if ok else "#33161a"
        badge_text = "#8ee3b7" if ok else "#ff9d9d"
        card_bg = "#0f1722"
        border = "#334155"
        title_color = "#e5eef8"
        detail_color = "#a7b6c8"
    else:
        badge_bg = "#d1fae5" if ok else "#fee2e2"
        badge_text = "#065f46" if ok else "#991b1b"
        card_bg = "rgba(255,255,255,0.8)"
        border = "rgba(31,41,55,0.08)"
        title_color = "#1f2937"
        detail_color = "#6b7280"
    badge_label = "Ready" if ok else "Pending"
    workflow_markup = (
        f'<div style="background:{card_bg};border:1px solid {border};border-radius:16px;padding:0.85rem 0.95rem;margin-bottom:0.55rem;">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;gap:0.75rem;">'
        f'<div style="font-size:0.95rem;font-weight:800;color:{title_color};">{title}</div>'
        f'<span style="background:{badge_bg};color:{badge_text};border-radius:999px;padding:0.22rem 0.55rem;font-size:0.78rem;font-weight:700;">{badge_label}</span>'
        f'</div>'
        f'<div style="margin-top:0.3rem;font-size:0.88rem;color:{detail_color};line-height:1.45;">{detail}</div>'
        f'</div>'
    )
    st.markdown(workflow_markup, unsafe_allow_html=True)


def format_freshness_label(raw_ts) -> str:
    if raw_ts is None or (isinstance(raw_ts, float) and pd.isna(raw_ts)):
        return "Last seen: unknown"
    try:
        ts = pd.to_datetime(raw_ts, utc=True)
        now = pd.Timestamp.now(tz="UTC")
        delta = now - ts
        seconds = max(int(delta.total_seconds()), 0)
        if seconds < 60:
            return "Last seen: just now"
        if seconds < 3600:
            minutes = max(1, seconds // 60)
            return f"Last seen: {minutes}m ago"
        if seconds < 86400:
            hours = max(1, seconds // 3600)
            return f"Last seen: {hours}h ago"
        days = max(1, seconds // 86400)
        return f"Last seen: {days}d ago"
    except Exception:
        return "Last seen: unknown"


def format_relative_timestamp(raw_ts, prefix: str = "") -> str:
    if raw_ts is None or (isinstance(raw_ts, float) and pd.isna(raw_ts)):
        return f"{prefix}unknown".strip()
    try:
        ts = pd.to_datetime(raw_ts, utc=True)
        now = pd.Timestamp.now(tz="UTC")
        delta = now - ts
        seconds = max(int(delta.total_seconds()), 0)
        if seconds < 60:
            label = "just now"
        elif seconds < 3600:
            label = f"{max(1, seconds // 60)}m ago"
        elif seconds < 86400:
            label = f"{max(1, seconds // 3600)}h ago"
        else:
            label = f"{max(1, seconds // 86400)}d ago"
        return f"{prefix}{label}".strip()
    except Exception:
        return f"{prefix}unknown".strip()


def format_pending_result_value(value, pending_label: str) -> str:
    if value is None:
        return pending_label
    if isinstance(value, float) and pd.isna(value):
        return pending_label
    value_text = str(value).strip()
    if not value_text or value_text.lower() == "none":
        return pending_label
    return value_text


def build_market_pulse_summary(edges_df: pd.DataFrame, watchlist_alerts_df: pd.DataFrame, is_dfs: bool) -> dict[str, str]:
    edge_count = int(len(edges_df)) if isinstance(edges_df, pd.DataFrame) else 0
    live_count = 0
    if isinstance(edges_df, pd.DataFrame) and not edges_df.empty and "coverage_status" in edges_df.columns:
        live_count = int((edges_df["coverage_status"].astype(str) == "Live").sum())
    alert_count = int(len(watchlist_alerts_df)) if isinstance(watchlist_alerts_df, pd.DataFrame) else 0
    board_label = "DFS board" if is_dfs else "sportsbook board"
    if edge_count <= 0:
        return {
            "title": "Thin board",
            "body": f"The {board_label} does not have ranked live edges yet, so sync health and market coverage still matter more than ticket building.",
        }
    if edge_count < 8 or live_count < 5:
        return {
            "title": "Selective board",
            "body": f"{edge_count} ranked edges are live with {live_count} provider-backed rows. This is a cleaner spot for shorter, more selective builds.",
        }
    if alert_count >= 3 or edge_count >= 18:
        return {
            "title": "Crowded board",
            "body": f"{edge_count} ranked edges and {alert_count} alert-ready spots are live. Let the smart ranker and watchlist do more filtering before you expand exposure.",
        }
    return {
        "title": "Healthy board",
        "body": f"{edge_count} ranked edges are available with {live_count} live-supported rows. This is a balanced window for scanning and building without forcing volume.",
    }


def build_sync_freshness_summary(last_sync, sync_enabled: bool) -> dict[str, str]:
    if not sync_enabled:
        return {
            "title": "Demo-backed sync",
            "body": "Live sync is not enabled for this board, so the app is leaning on demo-backed or previously seeded data.",
        }
    if last_sync is None:
        return {
            "title": "Sync not recent",
            "body": "No recent sync timestamp is available yet, so the board may need a fresh pull before trusting the latest live posture.",
        }
    try:
        ts = pd.to_datetime(last_sync, utc=True)
        now = pd.Timestamp.now(tz="UTC")
        minutes = max(int((now - ts).total_seconds() // 60), 0)
        if minutes <= 20:
            return {
                "title": "Fresh sync",
                "body": f"Last sync landed about {minutes} minute(s) ago, so the board is reasonably fresh for live scanning.",
            }
        if minutes <= 90:
            return {
                "title": "Aging sync",
                "body": f"Last sync was about {minutes} minute(s) ago. The board is still usable, but a refresh would tighten the read.",
            }
        return {
            "title": "Stale board risk",
            "body": f"Last sync was about {minutes} minute(s) ago, so live prices may have drifted away from the current board state.",
        }
    except Exception:
        return {
            "title": "Sync time unclear",
            "body": "The last sync timestamp could not be read cleanly, so treat the current board freshness with a little caution.",
        }


def build_risk_posture_summary(
    operating_mode: dict[str, str],
    weekly_review: dict[str, object],
    monthly_review: dict[str, object],
    is_dfs: bool,
) -> dict[str, str]:
    weekly_current = dict(weekly_review.get("current_summary") or {})
    monthly_current = dict(monthly_review.get("current_summary") or {})
    weekly_prior = dict(weekly_review.get("prior_summary") or {})
    weekly_units = float(weekly_current.get("profit_units", 0.0) or 0.0)
    monthly_units = float(monthly_current.get("profit_units", 0.0) or 0.0)
    weekly_hit_delta = float(weekly_current.get("hit_rate", 0.0) or 0.0) - float(weekly_prior.get("hit_rate", 0.0) or 0.0)
    mode_title = str(operating_mode.get("title") or "").lower()
    build_label = "DFS cards" if is_dfs else "sportsbook slips"
    if "press the edge" in mode_title or (weekly_units > 0 and monthly_units > 0 and weekly_hit_delta >= 0):
        return {
            "title": "Aggressive",
            "body": f"Recent results are supportive enough to let {build_label} stay a little more assertive, though the smart ranker should still do the filtering.",
        }
    if "selective mode" in mode_title or weekly_units < 0 or monthly_units < 0 or weekly_hit_delta < -0.04:
        return {
            "title": "Conservative",
            "body": f"Current review posture is cooler, so shorter builds and firmer confidence floors are the safer call for {build_label}.",
        }
    return {
        "title": "Balanced",
        "body": f"The board and recent review are mixed enough that balanced exposure is the better posture for current {build_label}.",
    }


def build_smart_mode_status_summary(
    source_summary_df: pd.DataFrame,
    smart_weight_profile: dict[str, object],
    manual_override_enabled: bool,
) -> dict[str, str]:
    profile_mode = str(smart_weight_profile.get("profile_mode") or "default").replace("_", " ").title()
    if manual_override_enabled:
        return {
            "title": "Manual smart mode",
            "body": f"Manual smart-engine overrides are active right now. Current tuning profile: {profile_mode}.",
        }
    if isinstance(source_summary_df, pd.DataFrame) and not source_summary_df.empty and "source" in source_summary_df.columns:
        auto_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_auto"].head(1)
        manual_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_manual"].head(1)
        if not auto_row.empty and not manual_row.empty:
            auto_roi = float(auto_row.iloc[0].get("roi_per_pick", 0.0) or 0.0)
            manual_roi = float(manual_row.iloc[0].get("roi_per_pick", 0.0) or 0.0)
            if auto_roi >= manual_roi + 0.05:
                return {
                    "title": "Auto mode leading",
                    "body": f"Smart Pick Engine (Auto) is currently ahead by {auto_roi - manual_roi:+.2f} units per pick, so auto-tuned scoring is earning the default seat.",
                }
            if manual_roi >= auto_roi + 0.05:
                return {
                    "title": "Manual test close",
                    "body": f"Manual smart scoring is edging auto by {manual_roi - auto_roi:+.2f} units per pick, so it is still worth monitoring before switching back.",
                }
    return {
        "title": "Auto smart mode",
        "body": f"Auto smart scoring is active with the current {profile_mode} profile, so the app is leaning on its learned weighting mix instead of manual overrides.",
    }


def build_posture_change_note(weekly_review: dict[str, object], monthly_review: dict[str, object]) -> str:
    weekly_current = dict(weekly_review.get("current_summary") or {})
    weekly_prior = dict(weekly_review.get("prior_summary") or {})
    monthly_current = dict(monthly_review.get("current_summary") or {})
    monthly_prior = dict(monthly_review.get("prior_summary") or {})
    weekly_hit_delta = float(weekly_current.get("hit_rate", 0.0) or 0.0) - float(weekly_prior.get("hit_rate", 0.0) or 0.0)
    monthly_units_delta = float(monthly_current.get("profit_units", 0.0) or 0.0) - float(monthly_prior.get("profit_units", 0.0) or 0.0)
    if weekly_hit_delta <= -0.04:
        return f"Posture cooled because weekly hit rate slipped by {weekly_hit_delta * 100:.1f} points versus the prior window."
    if weekly_hit_delta >= 0.04:
        return f"Posture firmed up because weekly hit rate improved by {weekly_hit_delta * 100:.1f} points versus the prior window."
    if monthly_units_delta <= -1.0:
        return f"Posture is leaning more selective because monthly profit is down {monthly_units_delta:+.2f} units versus the prior month."
    if monthly_units_delta >= 1.0:
        return f"Posture has a little more room because monthly profit improved by {monthly_units_delta:+.2f} units versus the prior month."
    return ""


def build_priority_badge_markup(label: str, tone: str) -> str:
    tone_map = {
        "good": "priority-strip__badge priority-strip__badge--good",
        "warn": "priority-strip__badge priority-strip__badge--warn",
        "alert": "priority-strip__badge priority-strip__badge--alert",
        "info": "priority-strip__badge priority-strip__badge--info",
        "neutral": "priority-strip__badge",
    }
    return f'<span class="{tone_map.get(tone, tone_map["neutral"])}">{label}</span>'


def render_top_priority_strip(
    operating_mode: dict[str, str],
    priority_cards: list[dict[str, str]],
    source_summary_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    watchlist_alerts_df: pd.DataFrame,
    weekly_review: dict[str, object],
    monthly_review: dict[str, object],
    smart_weight_profile: dict[str, object],
    manual_override_enabled: bool,
    last_sync,
    sync_enabled: bool,
    collapsed: bool,
    sport_label: str,
    is_dfs: bool,
) -> None:
    top_cards = priority_cards[:2]
    pulse = build_market_pulse_summary(edges_df, watchlist_alerts_df, is_dfs)
    freshness = build_sync_freshness_summary(last_sync, sync_enabled)
    risk_posture = build_risk_posture_summary(operating_mode, weekly_review, monthly_review, is_dfs)
    smart_mode = build_smart_mode_status_summary(source_summary_df, smart_weight_profile, manual_override_enabled)
    posture_change_note = build_posture_change_note(weekly_review, monthly_review)
    source_badges: list[str] = []
    if isinstance(source_summary_df, pd.DataFrame) and not source_summary_df.empty and "source" in source_summary_df.columns:
        for _, row in source_summary_df.head(2).iterrows():
            badge_label = format_source_label(str(row.get("source") or "workflow"))
            roi = float(row.get("roi_per_pick", 0.0) or 0.0)
            picks = int(row.get("picks", 0) or 0)
            source_badges.append(
                build_priority_badge_markup(f"{badge_label}: {roi:+.2f} u/pick over {picks} picks", "neutral")
            )
    freshness_tone = "info"
    if freshness.get("title") == "Fresh sync":
        freshness_tone = "good"
    elif freshness.get("title") in {"Aging sync", "Sync not recent", "Sync time unclear"}:
        freshness_tone = "warn"
    elif freshness.get("title") == "Stale board risk":
        freshness_tone = "alert"
    risk_tone = {"Aggressive": "good", "Balanced": "info", "Conservative": "warn"}.get(risk_posture.get("title", ""), "neutral")
    smart_tone = "warn" if manual_override_enabled or smart_mode.get("title") == "Manual test close" else "good"
    posture_label = "DFS posture" if is_dfs else f"{sport_label} posture".strip()
    cards_markup: list[str] = []
    for card in top_cards:
        cards_markup.append(
            (
                f'<div class="priority-strip__card">'
                f'<div class="priority-strip__eyebrow">{card.get("status", "priority")}</div>'
                f'<div class="priority-strip__title">{card.get("title", "")}</div>'
                f'<div class="priority-strip__body">{card.get("body", "")}</div>'
                f"</div>"
            )
        )
    badges_markup = (
        build_priority_badge_markup(f"Freshness: {freshness.get('title', '')}", freshness_tone)
        + build_priority_badge_markup(f"Risk: {risk_posture.get('title', '')}", risk_tone)
        + build_priority_badge_markup(f"Smart: {smart_mode.get('title', '')}", smart_tone)
        + "".join(source_badges)
    )
    cards_section = f'<div class="priority-strip__cards">{"".join(cards_markup)}</div>' if not collapsed else ""
    detail_section = (
        f'<div class="priority-strip__body" style="margin-top:0.75rem;">{freshness.get("body", "")}</div>'
        f'<div class="priority-strip__body" style="margin-top:0.45rem;">{risk_posture.get("body", "")}</div>'
        f'<div class="priority-strip__body" style="margin-top:0.45rem;">{smart_mode.get("body", "")}</div>'
    )
    if posture_change_note and not collapsed:
        detail_section += f'<div class="priority-strip__body" style="margin-top:0.55rem;font-style:italic;">{posture_change_note}</div>'
    priority_markup = (
        f'<div class="priority-strip">'
        f'<div class="priority-strip__mode">'
        f'<div class="priority-strip__eyebrow">{posture_label}</div>'
        f'<div class="priority-strip__mode-title">{operating_mode.get("title", "")}</div>'
        f'<div class="priority-strip__body">{operating_mode.get("body", "")}</div>'
        f'<div class="priority-strip__pulse">'
        f'<strong>{pulse.get("title", "")}</strong>'
        f'<span>{pulse.get("body", "")}</span>'
        f'</div>'
        f'<div class="priority-strip__badges">{badges_markup}</div>'
        f'{detail_section}'
        f'</div>'
        f'{cards_section}'
        f'</div>'
    )
    st.markdown(priority_markup, unsafe_allow_html=True)

    top_action_cols = st.columns([1.1, 1, 1, 1])
    default_target_map = {
        "Edge Scanner": "edge_scanner",
        "Parlay Lab": "parlay_lab",
        "Results & Grading": "results_grading",
        "Backtest": "backtest",
        "Live Board": "live_board",
    }
    if top_action_cols[0].button(
        "Expand Priorities" if collapsed else "Collapse Priorities",
        key="top_priority_toggle",
        use_container_width=True,
    ):
        st.session_state["top_priority_strip_collapsed"] = not collapsed
        persist_preference_if_changed("__app__", "top_priority_strip_collapsed", not collapsed, False)
        st.rerun()
    if top_action_cols[1].button(
        f"Focus {operating_mode.get('default_workflow', 'Overview')}",
        key="top_priority_focus_mode",
        use_container_width=True,
    ):
        target = default_target_map.get(str(operating_mode.get("default_workflow") or ""), "overview")
        set_dashboard_focus(target)
        if target == "results_grading":
            set_results_grading_focus("saved_tickets")
        st.rerun()
    if top_cards:
        first_card = top_cards[0]
        first_label = str(first_card.get("action_label") or "Use Top Priority")
        if top_action_cols[2].button(first_label, key="top_priority_card_one", use_container_width=True):
            handle_recommendation_card_action(first_card)
    else:
        top_action_cols[2].empty()
    if len(top_cards) > 1:
        second_card = top_cards[1]
        second_label = str(second_card.get("action_label") or "Use Next Priority")
        if top_action_cols[3].button(second_label, key="top_priority_card_two", use_container_width=True):
            handle_recommendation_card_action(second_card)
    else:
        top_action_cols[3].empty()


def build_overview_next_step_cards(
    overview_board: pd.DataFrame,
    overview_edges: pd.DataFrame,
    overview_watchlist_alerts: pd.DataFrame,
    overview_tickets: pd.DataFrame,
    overview_unresolved_tracked: pd.DataFrame,
    overview_graded: pd.DataFrame,
    overview_journal: pd.DataFrame,
    source_summary_df: pd.DataFrame,
    weekly_review: dict[str, object],
    monthly_review: dict[str, object],
    sport_label: str,
    is_dfs: bool,
) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    weekly_current = dict(weekly_review.get("current_summary") or {})
    monthly_current = dict(monthly_review.get("current_summary") or {})
    weekly_prior = dict(weekly_review.get("prior_summary") or {})
    weekly_units = float(weekly_current.get("profit_units", 0.0) or 0.0)
    monthly_units = float(monthly_current.get("profit_units", 0.0) or 0.0)
    weekly_hit_delta = float(weekly_current.get("hit_rate", 0.0) or 0.0) - float(weekly_prior.get("hit_rate", 0.0) or 0.0)
    sport_prefix = f"{sport_label} " if str(sport_label).strip() else ""
    board_label = "DFS board" if is_dfs else "sportsbook board"
    build_label = "DFS auto-slip and card builder" if is_dfs else "Parlay Lab"
    settlement_label = "Results & Grading" if not is_dfs else "ticket review and result entry"
    review_ready = not overview_graded.empty
    positive_trend = weekly_units > 0 and monthly_units >= 0
    cooling_trend = weekly_units < 0 or monthly_units < 0 or weekly_hit_delta < -0.04
    source_summary_df = source_summary_df.copy() if isinstance(source_summary_df, pd.DataFrame) else pd.DataFrame()

    def confidence_label(level: str) -> str:
        return level

    if overview_board.empty:
        cards.append(
            {
                "title": f"Start With {sport_prefix}{'DFS ' if is_dfs else ''}Sync Or Demo Seed".strip(),
                "status": "setup",
                "confidence": confidence_label("High Confidence"),
                "body": f"The {board_label} is still empty, so the best next move is loading fresh market rows before working in Edge Scanner or {build_label}.",
                "action_label": "Focus Live Board",
                "action_target": "live_board",
            }
        )
    elif overview_edges.empty:
        cards.append(
            {
                "title": "Check Edge Scanner Next",
                "status": "data ready",
                "confidence": confidence_label("Medium Confidence"),
                "body": f"{sport_prefix}{board_label.capitalize()} rows are available, but edge-ranked picks are still thin. Review Edge Scanner after the next sync or loosen filters if you are testing locally.",
                "action_label": "Focus Edge Scanner",
                "action_target": "edge_scanner",
            }
        )
    elif overview_watchlist_alerts.empty:
        cards.append(
            {
                "title": "Promote Strong Edges Into Watchlist",
                "status": "builder",
                "confidence": confidence_label("Medium Confidence"),
                "body": f"The {board_label} and edge model are live. The next useful step is saving a few props to the watchlist so alerts and {build_label.lower()} workflows have stronger context.",
                "action_label": "Focus Edge Scanner",
                "action_target": "edge_scanner",
            }
        )
    else:
        cards.append(
            {
                "title": f"{build_label} Is Ready",
                "status": "ready" if positive_trend else "builder",
                "confidence": confidence_label("High Confidence" if len(overview_watchlist_alerts) >= 2 else "Medium Confidence"),
                "body": f"{len(overview_watchlist_alerts)} current watchlist alert(s) are available, so {build_label} is a strong next stop for building or comparing {sport_prefix.lower()}tickets.",
                "action_label": "Focus Parlay Lab",
                "action_target": "parlay_lab",
            }
        )

    if overview_tickets.empty:
        cards.append(
            {
                "title": "Save Your First Ticket",
                "status": "tracking",
                "confidence": confidence_label("High Confidence"),
                "body": f"Once you like a build, save a ticket from {build_label} so {settlement_label}, ticket review, and bankroll tracking can start learning from it.",
                "action_label": "Focus Parlay Lab",
                "action_target": "parlay_lab",
            }
        )
    elif not overview_unresolved_tracked.empty:
        cards.append(
            {
                "title": "Work The Settlement Queue",
                "status": "needs action",
                "confidence": confidence_label("High Confidence"),
                "body": f"{len(overview_unresolved_tracked)} tracked pick(s) are still open, so Results & Grading is the best place to settle or sync {sport_prefix.lower()}outcomes next.",
                "action_label": "Focus Settlement Queue",
                "action_target": "results_grading",
                "action_section_target": "enter_settled_result",
            }
        )
    elif overview_graded.empty:
        cards.append(
            {
                "title": "Build Graded History",
                "status": "learning",
                "confidence": confidence_label("Medium Confidence"),
                "body": f"You have {sport_prefix.lower()}tickets saved, but the smart engine still needs settled outcomes. Sync or enter results so the review and learning layers can strengthen.",
                "action_label": "Focus Results & Grading",
                "action_target": "results_grading",
                "action_section_target": "saved_tickets",
            }
        )
    else:
        cards.append(
            {
                "title": "Review Model Performance",
                "status": "analysis" if review_ready else "learning",
                "confidence": confidence_label("High Confidence"),
                "body": f"{len(overview_graded)} graded {sport_prefix.lower()}pick(s) are ready, so Weekly Review, Monthly Review, and Backtest are now worth checking for the next tuning call.",
                "action_label": "Focus Backtest",
                "action_target": "backtest",
            }
        )

    if overview_journal.empty:
        cards.append(
            {
                "title": "Activate Bankroll Journal",
                "status": "optional",
                "confidence": confidence_label("Medium Confidence"),
                "body": "Bankroll tracking has not started yet. Logging manual bets or saved tickets will unlock ROI, yield, and bankroll trend visibility.",
                "action_label": "Focus Bankroll Journal",
                "action_target": "bankroll_journal",
                "action_section_target": "bankroll_journal",
            }
        )
    elif cooling_trend:
        cards.append(
            {
                "title": f"Stay Selective On {sport_prefix}Volume".strip(),
                "status": "trend watch",
                "confidence": confidence_label("Medium Confidence"),
                "body": f"Weekly and monthly review are leaning cooler right now, so it makes sense to keep {sport_prefix.lower()}stakes cleaner, shorter, and more selective until the trend firms back up.",
                "action_label": "Focus Backtest",
                "action_target": "backtest",
            }
        )
    elif positive_trend:
        cards.append(
            {
                "title": f"Lean Into The Strong {sport_prefix}Window".strip(),
                "status": "positive trend",
                "confidence": confidence_label("Medium Confidence"),
                "body": f"Weekly and monthly review are both supportive right now, so this is a healthier stretch to trust the current smart mix and let the strongest {sport_prefix.lower()}workflows do more of the lifting.",
                "action_label": "Focus Parlay Lab",
                "action_target": "parlay_lab",
            }
        )

    if not source_summary_df.empty and "source" in source_summary_df.columns:
        auto_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_auto"].head(1)
        manual_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_manual"].head(1)
        legacy_row = source_summary_df[source_summary_df["source"] == "edge_scanner"].head(1)
        source_card = None
        if not auto_row.empty and not legacy_row.empty:
            auto_units = float(auto_row.iloc[0].get("roi_per_pick", 0.0) or 0.0)
            legacy_units = float(legacy_row.iloc[0].get("roi_per_pick", 0.0) or 0.0)
            if auto_units > legacy_units + 0.08:
                source_card = {
                    "title": "Auto Smart Is Leading Today",
                    "status": "source edge",
                    "confidence": confidence_label("Medium Confidence"),
                    "body": f"Smart Pick Engine (Auto) is outperforming Edge Scanner by {auto_units - legacy_units:+.2f} units per pick in graded history, so it is the stronger default workflow right now.",
                    "action_label": "Focus Edge Scanner",
                    "action_target": "edge_scanner",
                }
        if source_card is None and not manual_row.empty and not auto_row.empty:
            manual_units = float(manual_row.iloc[0].get("roi_per_pick", 0.0) or 0.0)
            auto_units = float(auto_row.iloc[0].get("roi_per_pick", 0.0) or 0.0)
            if manual_units > auto_units + 0.08:
                source_card = {
                    "title": "Manual Smart Has The Edge",
                    "status": "source edge",
                    "confidence": confidence_label("Medium Confidence"),
                    "body": f"Smart Pick Engine (Manual) is ahead of Auto by {manual_units - auto_units:+.2f} units per pick, so manual tuning is still earning its keep for this board.",
                    "action_label": "Focus Results & Grading",
                    "action_target": "results_grading",
                    "action_section_target": "smart_pick_learning",
                }
        if source_card is None and not legacy_row.empty and legacy_row.iloc[0].get("picks", 0):
            source_card = {
                "title": "Legacy Scanner Still Matters",
                "status": "source watch",
                "confidence": confidence_label("Low Confidence"),
                "body": f"{format_source_label(str(legacy_row.iloc[0].get('source') or 'edge_scanner'))} still has meaningful graded history here, so keep comparing it against the smart workflows before going all-in on one lane.",
                "action_label": "Focus Edge Scanner",
                "action_target": "edge_scanner",
            }
        if source_card is not None:
            cards.insert(1, source_card)
    return cards[:3]


def build_overview_operating_mode(
    source_summary_df: pd.DataFrame,
    weekly_review: dict[str, object],
    monthly_review: dict[str, object],
    sport_label: str,
    is_dfs: bool,
    overview_watchlist_alerts: pd.DataFrame,
    overview_unresolved_tracked: pd.DataFrame,
    overview_edges: pd.DataFrame,
) -> dict[str, str]:
    weekly_current = dict(weekly_review.get("current_summary") or {})
    monthly_current = dict(monthly_review.get("current_summary") or {})
    weekly_prior = dict(weekly_review.get("prior_summary") or {})
    weekly_units = float(weekly_current.get("profit_units", 0.0) or 0.0)
    monthly_units = float(monthly_current.get("profit_units", 0.0) or 0.0)
    weekly_hit_delta = float(weekly_current.get("hit_rate", 0.0) or 0.0) - float(weekly_prior.get("hit_rate", 0.0) or 0.0)
    sport_prefix = f"{sport_label} " if str(sport_label).strip() else ""
    build_label = "DFS builder" if is_dfs else "sportsbook builder"
    default_workflow = "Edge Scanner"
    confidence = "Medium Confidence"
    status = "Balanced Mode"
    body = f"The current {sport_prefix.lower()}operating mode is balanced. Start in Edge Scanner, then move into {build_label} once a few clean candidates surface."

    if not overview_unresolved_tracked.empty:
        default_workflow = "Results & Grading"
        confidence = "High Confidence"
        status = "Queue First"
        body = f"{len(overview_unresolved_tracked)} tracked {sport_prefix.lower()}pick(s) are still waiting for settlement, so Results & Grading should come first before expanding the next build cycle."
    elif weekly_units > 0 and monthly_units >= 0 and not overview_watchlist_alerts.empty:
        default_workflow = "Parlay Lab"
        confidence = "High Confidence"
        status = "Press The Edge"
        body = f"Weekly and monthly trend are both supportive, and {len(overview_watchlist_alerts)} watchlist alert(s) are live. {build_label.title()} is the strongest first workflow right now."
    elif weekly_units < 0 or monthly_units < 0 or weekly_hit_delta < -0.04:
        default_workflow = "Backtest"
        confidence = "Medium Confidence"
        status = "Selective Mode"
        body = f"The recent {sport_prefix.lower()}trend is cooling, so Backtest and review are the best first stop before forcing new volume."
    elif overview_edges.empty:
        default_workflow = "Live Board"
        confidence = "High Confidence"
        status = "Setup Mode"
        body = f"The {sport_prefix.lower()}edge pool is still thin, so Live Board is the right starting point for checking market coverage and sync health."

    if isinstance(source_summary_df, pd.DataFrame) and not source_summary_df.empty and "source" in source_summary_df.columns:
        top_source = str(source_summary_df.iloc[0].get("source") or "")
        if top_source:
            body = f"{body} Current workflow leader: {format_source_label(top_source)}."

    return {
        "title": f"Today's Operating Mode: {status}",
        "confidence": confidence,
        "default_workflow": default_workflow,
        "body": body,
    }


def build_watchlist_alert_reason(row: pd.Series, threshold_edge_pct: float, threshold_confidence: float) -> str:
    edge_pct = float(row.get("edge", 0.0) or 0.0) * 100
    confidence = float(row.get("confidence", 0.0) or 0.0)
    line_label = str(row.get("line_move_label") or "neutral")
    price_label = str(row.get("price_move_label") or "neutral")

    movement_bits = []
    if line_label != "neutral":
        movement_bits.append(f"line is {line_label}")
    if price_label != "neutral":
        movement_bits.append(f"price is {price_label}")

    movement_text = ", ".join(movement_bits) if movement_bits else "market is stable"
    return (
        f"Alerted because edge is {edge_pct:.1f}% vs {threshold_edge_pct:.1f}% target, "
        f"confidence is {confidence:.1f} vs {threshold_confidence:.1f}, and the {movement_text}."
    )


def render_watchlist_alert_card(row: pd.Series) -> None:
    line_label = str(row.get("line_move_label") or "neutral")
    price_label = str(row.get("price_move_label") or "neutral")
    line_color_map = (
        {
            "better": ("#8ee3b7", "#0f2b1f"),
            "worse": ("#ff9d9d", "#33161a"),
            "neutral": ("#dbe4f0", "#1f2937"),
        }
        if theme_mode == "Dark"
        else {
            "better": ("#065f46", "#d1fae5"),
            "worse": ("#991b1b", "#fee2e2"),
            "neutral": ("#374151", "#e5e7eb"),
        }
    )
    price_color_map = line_color_map
    line_text_color, line_bg = line_color_map.get(
        line_label,
        ("#dbe4f0", "#1f2937") if theme_mode == "Dark" else ("#374151", "#e5e7eb"),
    )
    price_text_color, price_bg = price_color_map.get(
        price_label,
        ("#dbe4f0", "#1f2937") if theme_mode == "Dark" else ("#374151", "#e5e7eb"),
    )
    edge_pct = float(row.get("edge", 0.0) or 0.0) * 100
    confidence = float(row.get("confidence", 0.0) or 0.0)
    recommended_units = float(row.get("recommended_units", 0.0) or 0.0)
    freshness_label = format_freshness_label(row.get("pulled_at") or row.get("last_update"))
    player_label = row.get("player_display") or row.get("player", "Unknown")
    watchlist_markup = (
        f'<div class="watchlist-alert-card">'
        f'<div class="watchlist-alert-card__title">{player_label} - {row.get("market", "")}</div>'
        f'<div class="watchlist-alert-card__subtitle">{row.get("pick", "")} | {row.get("sportsbook", "")}</div>'
        f'<div class="watchlist-alert-card__metrics">'
        f'<div><span>Edge</span><strong>{edge_pct:.2f}%</strong></div>'
        f'<div><span>Confidence</span><strong>{confidence:.1f}</strong></div>'
        f'<div><span>Stake</span><strong>{recommended_units:.2f}u</strong></div>'
        f'</div>'
        f'<div class="watchlist-alert-card__signals">'
        f'<span style="background:{line_bg};color:{line_text_color};">Line: {line_label}</span>'
        f'<span style="background:{price_bg};color:{price_text_color};">Price: {price_label}</span>'
        f'</div>'
        f'<div class="watchlist-alert-card__freshness">{freshness_label}</div>'
        f'</div>'
    )
    st.markdown(watchlist_markup, unsafe_allow_html=True)


def render_watchlist_alert_card_with_reason(row: pd.Series, threshold_edge_pct: float, threshold_confidence: float) -> None:
    render_watchlist_alert_card(row)
    st.caption(build_watchlist_alert_reason(row, threshold_edge_pct, threshold_confidence))


def build_notification_center(
    watchlist_alerts: pd.DataFrame,
    unresolved_tracked: pd.DataFrame,
    saved_tickets: pd.DataFrame,
    journal_df: pd.DataFrame,
) -> list[dict]:
    notices: list[dict] = []

    if not watchlist_alerts.empty:
        top_alert = watchlist_alerts.sort_values(["confidence", "edge"], ascending=False).iloc[0]
        player_label = str(top_alert.get("player_display") or top_alert.get("player") or "A watched prop")
        notices.append(
            {
                "notice_id": "watchlist_alert_live",
                "priority": 1,
                "severity": "high",
                "title": "Watchlist alert live",
                "message": (
                    f"{player_label} {top_alert.get('pick', '')} "
                    f"is live with {float(top_alert.get('edge', 0.0) or 0.0) * 100:.1f}% edge "
                    f"at {float(top_alert.get('confidence', 0.0) or 0.0):.1f} confidence."
                ).strip(),
                "action_label": "Focus Parlay Lab",
                "action_target": "parlay_lab",
            }
        )

    if not unresolved_tracked.empty:
        notices.append(
            {
                "notice_id": "tracked_picks_waiting",
                "priority": 2,
                "severity": "medium",
                "title": "Tracked picks waiting on grading",
                "message": f"{len(unresolved_tracked)} tracked picks are still unresolved and may be ready for auto-settle soon.",
                "action_label": "Focus Results & Grading",
                "action_target": "results_grading",
            }
        )

    if not saved_tickets.empty:
        open_tickets = saved_tickets[saved_tickets["ticket_status_live"] == "open"].copy() if "ticket_status_live" in saved_tickets.columns else pd.DataFrame()
        if not open_tickets.empty:
            notices.append(
                {
                    "notice_id": "open_saved_tickets",
                    "priority": 3,
                    "severity": "low",
                    "title": "Open saved tickets",
                    "message": f"{len(open_tickets)} saved tickets are still open and worth checking after the next sync.",
                    "action_label": "Focus Results & Grading",
                    "action_target": "results_grading",
                }
            )

    if not journal_df.empty:
        open_manual_entries = journal_df[
            (journal_df["status"] == "open") & (journal_df["entry_type"] == "manual")
        ].copy()
        if not open_manual_entries.empty:
            notices.append(
                {
                    "notice_id": "manual_bankroll_settlement",
                    "priority": 4,
                    "severity": "medium",
                    "title": "Manual bankroll items need settlement",
                    "message": f"{len(open_manual_entries)} manual bankroll entries are still open and may need realized P/L entered.",
                    "action_label": "Focus Bankroll Journal",
                    "action_target": "bankroll_journal",
                }
            )

    return sorted(notices, key=lambda item: item["priority"])


def render_notification_notice(notice: dict, key_suffix: str, sport_label: str) -> None:
    severity_map = (
        {
            "high": {"fg": "#ffd7d7", "bg": "linear-gradient(135deg, rgba(56, 24, 35, 0.96), rgba(91, 30, 48, 0.92))", "border": "#a44d68", "pill_bg": "rgba(255, 159, 181, 0.14)", "pill_text": "#ffb7cb"},
            "medium": {"fg": "#ffe8b3", "bg": "linear-gradient(135deg, rgba(51, 39, 15, 0.95), rgba(84, 60, 18, 0.92))", "border": "#a77a26", "pill_bg": "rgba(245, 210, 122, 0.14)", "pill_text": "#f9d98d"},
            "low": {"fg": "#dceeff", "bg": "linear-gradient(135deg, rgba(13, 33, 57, 0.97), rgba(21, 55, 97, 0.93))", "border": "#3d79cb", "pill_bg": "rgba(125, 211, 252, 0.12)", "pill_text": "#9ddcff"},
        }
        if theme_mode == "Dark"
        else {
            "high": {"fg": "#7f1d1d", "bg": "#fee2e2", "border": "#fecaca", "pill_bg": "#fff1f2", "pill_text": "#9f1239"},
            "medium": {"fg": "#92400e", "bg": "#fef3c7", "border": "#fde68a", "pill_bg": "#fffbeb", "pill_text": "#a16207"},
            "low": {"fg": "#1e3a8a", "bg": "#dbeafe", "border": "#bfdbfe", "pill_bg": "#eff6ff", "pill_text": "#2563eb"},
        }
    )
    tone = severity_map.get(
        str(notice.get("severity") or "low"),
        {"fg": "#dbe4f0", "bg": "#1f2937", "border": "#334155", "pill_bg": "#0f172a", "pill_text": "#dbe4f0"}
        if theme_mode == "Dark"
        else {"fg": "#374151", "bg": "#f3f4f6", "border": "#e5e7eb", "pill_bg": "#ffffff", "pill_text": "#374151"},
    )
    notice_markup = (
        f'<div style="background:{tone["bg"]};border:1px solid {tone["border"]};color:{tone["fg"]};border-radius:18px;padding:0.9rem 1rem;margin-bottom:0.6rem;box-shadow:0 14px 30px rgba(2, 8, 23, 0.16);">'
        f'<div style="display:flex;align-items:center;justify-content:space-between;gap:0.8rem;margin-bottom:0.35rem;">'
        f'<div style="font-size:0.95rem;font-weight:800;">{notice["title"]}</div>'
        f'<span style="display:inline-block;padding:0.24rem 0.55rem;border-radius:999px;background:{tone["pill_bg"]};color:{tone["pill_text"]};font-size:0.74rem;font-weight:800;text-transform:uppercase;letter-spacing:0.05em;white-space:nowrap;">{str(notice.get("severity") or "low")}</span>'
        f'</div>'
        f'<div style="font-size:0.9rem;line-height:1.5;opacity:0.96;">{notice["message"]}</div>'
        f'</div>'
    )
    st.markdown(notice_markup, unsafe_allow_html=True)
    action_col, snooze_col, dismiss_col = st.columns([1.3, 1, 1])
    if notice.get("action_label") and notice.get("action_target"):
        if action_col.button(str(notice["action_label"]), key=f"notice_action_{key_suffix}", use_container_width=True):
            set_dashboard_focus(str(notice["action_target"]))
            st.rerun()
    if snooze_col.button("Snooze 24h", key=f"notice_snooze_{key_suffix}", use_container_width=True):
        snooze_notification(sport_label, str(notice["notice_id"]), hours=24)
        st.rerun()
    if dismiss_col.button("Dismiss", key=f"notice_dismiss_{key_suffix}", use_container_width=True):
        dismiss_notification(sport_label, str(notice["notice_id"]))
        st.rerun()

def render_prop_card(card: dict) -> None:
    accent_map = {
        "Live": "#2a9d8f",
        "Demo Only": "#e9c46a",
        "Provider Unavailable": "#e76f51",
    }
    accent = accent_map.get(card.get("coverage_status"), "#264653")
    if theme_mode == "Dark":
        card_bg = "rgba(15,23,34,0.96)"
        border = "#334155"
        shadow = "0 14px 32px rgba(2,6,23,0.35)"
        title_color = "#e5eef8"
        muted_color = "#a7b6c8"
        note_color = "#cbd5e1"
    else:
        card_bg = "rgba(255,255,255,0.86)"
        border = "rgba(31,41,55,0.08)"
        shadow = "0 12px 28px rgba(15,23,42,0.06)"
        title_color = "#1f2937"
        muted_color = "#6b7280"
        note_color = "#4b5563"
    st.markdown(
        f"""
        <div style="
            background: {card_bg};
            border: 1px solid {border};
            border-left: 6px solid {accent};
            border-radius: 18px;
            padding: 1rem 1rem 0.9rem 1rem;
            box-shadow: {shadow};
            margin-bottom: 0.85rem;
        ">
            <div style="display:flex;justify-content:space-between;gap:1rem;align-items:flex-start;">
                <div>
                    <div style="font-size:1.03rem;font-weight:800;color:{title_color};">{card['title']}</div>
                    <div style="font-size:0.9rem;color:{muted_color};margin-top:0.15rem;">{card['sportsbook']} | {card['pick']}</div>
                </div>
                <div>{render_coverage_badge(card["coverage_status"])}</div>
            </div>
            <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0.65rem;margin-top:0.85rem;">
                <div><div style="font-size:0.72rem;color:{muted_color};text-transform:uppercase;">Projection</div><div style="font-size:1rem;font-weight:700;color:{title_color};">{card['projection']}</div></div>
                <div><div style="font-size:0.72rem;color:{muted_color};text-transform:uppercase;">Model Prob</div><div style="font-size:1rem;font-weight:700;color:{title_color};">{card['model_prob']}%</div></div>
                <div><div style="font-size:0.72rem;color:{muted_color};text-transform:uppercase;">Edge</div><div style="font-size:1rem;font-weight:700;color:{title_color};">{card['edge']}%</div></div>
                <div><div style="font-size:0.72rem;color:{muted_color};text-transform:uppercase;">Stake</div><div style="font-size:1rem;font-weight:700;color:{title_color};">{card['recommended_units']}u</div></div>
            </div>
            <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:0.65rem;margin-top:0.75rem;">
                <div><div style="font-size:0.72rem;color:{muted_color};text-transform:uppercase;">Implied Prob</div><div style="font-size:0.96rem;font-weight:600;color:{title_color};">{card['implied_prob']}%</div></div>
                <div><div style="font-size:0.72rem;color:{muted_color};text-transform:uppercase;">Consensus</div><div style="font-size:0.96rem;font-weight:600;color:{title_color};">{card['consensus_line']}</div></div>
                <div><div style="font-size:0.72rem;color:{muted_color};text-transform:uppercase;">Confidence</div><div style="font-size:0.96rem;font-weight:600;color:{title_color};">{card['confidence']}</div></div>
            </div>
            <div style="margin-top:0.7rem;font-size:0.86rem;color:{note_color};">{card.get("coverage_note") or ""}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dfs_autoslip_panel(
    card_df: pd.DataFrame,
    sport_label: str,
    source_label: str,
    style_label: str,
    key_prefix: str,
) -> None:
    if card_df.empty:
        return None

    adapters = get_dfs_slip_adapters()
    adapter_options = {adapter["label"]: adapter for adapter in adapters}
    recommendation = recommend_dfs_slip_adapter(card_df, style_label=style_label)
    recommended_adapter = recommendation["adapter"]
    default_label = str(recommended_adapter["label"])
    recommendation_bg = "rgba(13, 25, 44, 0.72)" if theme_mode == "Dark" else "rgba(255, 255, 255, 0.9)"
    recommendation_border = "rgba(96, 165, 250, 0.14)" if theme_mode == "Dark" else "rgba(31, 41, 55, 0.08)"
    recommendation_title = "#f3f8ff" if theme_mode == "Dark" else "#17324d"
    recommendation_body = "#c9dcf3" if theme_mode == "Dark" else "#526273"

    st.markdown("### DFS Auto-Slip")
    st.markdown(
        f"""
        <div style="
            background: {recommendation_bg};
            border: 1px solid {recommendation_border};
            border-radius: 18px;
            padding: 0.9rem 1rem;
            margin-bottom: 0.8rem;
        ">
            <div style="display:flex;align-items:center;gap:0.7rem;margin-bottom:0.25rem;">
                <span style="
                    display:inline-flex;
                    align-items:center;
                    justify-content:center;
                    width:2rem;
                    height:2rem;
                    border-radius:999px;
                    background:{recommended_adapter['accent']};
                    color:#f8fbff;
                    font-size:0.82rem;
                    font-weight:800;
                ">{recommended_adapter['brand_mark']}</span>
                <div style="font-size:1rem;font-weight:800;color:{recommendation_title};">Best Current Destination: {recommended_adapter['label']}</div>
            </div>
            <div style="font-size:0.9rem;color:{recommendation_body};line-height:1.5;">{recommendation['reason']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    target_label = st.selectbox(
        "DFS target app",
        options=list(adapter_options.keys()),
        index=list(adapter_options.keys()).index(default_label),
        key=f"{key_prefix}_dfs_target_app",
    )
    adapter = adapter_options[target_label]
    payload = build_dfs_slip_payload(
        card_df=card_df,
        adapter_key=str(adapter["key"]),
        sport_label=sport_label,
        source_label=source_label,
        style_label=style_label,
    )
    slip_text = format_dfs_slip_text(card_df, target_label)
    slip_payload_text = format_dfs_slip_payload(payload)
    slip_json = format_dfs_slip_json(payload)

    meta_col1, meta_col2, meta_col3 = st.columns(3)
    meta_col1.metric("Target", f"{adapter['brand_mark']} {target_label}")
    meta_col2.metric("Legs", str(payload["leg_count"]))
    meta_col3.metric("Prefill", "Available" if adapter["supports_public_prefill"] else "Not public")

    if adapter["supports_public_prefill"]:
        st.success("This target supports direct external prefill. Submission still happens on the destination platform.")
    else:
        st.info(f"{adapter['notes']} Submission still happens on the destination platform.")

    st.caption(
        "AI Parlay Builder generates the recommended DFS slip here. If the target app exposes public prefill support, we can send it preloaded. Otherwise, the app opens with a ready-to-load payload and the user completes submission there."
    )

    launch_col, payload_col, json_col = st.columns(3)
    launch_col.link_button(f"Open {target_label}", str(adapter["launch_url"]), use_container_width=True)
    payload_col.download_button(
        "Download slip text",
        data=slip_text,
        file_name=f"{sport_label.lower()}_{adapter['key']}_dfs_slip.txt",
        mime="text/plain",
        use_container_width=True,
        key=f"{key_prefix}_dfs_download_text",
    )
    json_col.download_button(
        "Download payload JSON",
        data=slip_json,
        file_name=f"{sport_label.lower()}_{adapter['key']}_dfs_slip.json",
        mime="application/json",
        use_container_width=True,
        key=f"{key_prefix}_dfs_download_json",
    )

    with st.expander("Ready-to-load payload", expanded=False):
        st.caption("Use this when the target app supports easy manual recreation, sharing, or copy-tail workflows but does not publish a public external prefill API.")
        st.text_area(
            "Slip summary",
            value=slip_payload_text,
            height=180,
            key=f"{key_prefix}_dfs_payload_text",
        )
        st.download_button(
            "Download full payload JSON",
            data=slip_json,
            file_name=f"{sport_label.lower()}_{adapter['key']}_dfs_payload.json",
            mime="application/json",
            use_container_width=True,
            key=f"{key_prefix}_dfs_payload_download_inline",
        )

    with st.expander("Supported DFS adapters", expanded=False):
        adapter_df = pd.DataFrame(adapters)[["label", "handoff_mode", "supports_public_prefill", "supports_web_entry", "submission_mode", "notes"]]
        adapter_df = adapter_df.rename(
            columns={
                "label": "App",
                "handoff_mode": "Handoff mode",
                "supports_public_prefill": "Public prefill",
                "supports_web_entry": "Web entry",
                "submission_mode": "Submission",
                "notes": "Notes",
            }
        )
        st.dataframe(adapter_df, use_container_width=True, hide_index=True)
    return adapter


def ticket_looks_like_dfs(ticket_row: pd.Series | None, legs_df: pd.DataFrame) -> bool:
    if ticket_row is not None and str(ticket_row.get("dfs_target_key") or "").strip():
        return True
    if legs_df.empty:
        return False
    adapters = get_dfs_slip_adapters()
    adapter_tokens: set[str] = set()
    for adapter in adapters:
        adapter_tokens.update(
            {
                str(adapter["key"]).lower(),
                str(adapter["label"]).lower(),
                str(adapter["label"]).lower().replace(" fantasy", ""),
            }
        )
    for column in ["sportsbook", "book_key"]:
        if column not in legs_df.columns:
            continue
        for value in legs_df[column].dropna().tolist():
            lowered = str(value).strip().lower()
            if lowered and any(token in lowered for token in adapter_tokens):
                return True
    return False


def format_probability_bucket_label(bucket_value) -> str:
    if bucket_value is None or (isinstance(bucket_value, float) and pd.isna(bucket_value)):
        return ""
    left = getattr(bucket_value, "left", None)
    right = getattr(bucket_value, "right", None)
    if left is not None and right is not None and pd.notna(left) and pd.notna(right):
        return f"{float(left) * 100:.0f}% to {float(right) * 100:.0f}%"
    raw_text = str(bucket_value).strip()
    if raw_text.startswith("{") and "left" in raw_text and "right" in raw_text:
        try:
            normalized = json.loads(raw_text.replace("'", "\""))
            left = float(normalized.get("left"))
            right = float(normalized.get("right"))
            return f"{left * 100:.0f}% to {right * 100:.0f}%"
        except Exception:
            return raw_text
    return raw_text


def promote_saved_ticket_to_parlay_lab(ticket_id: int, ticket_name: str, ticket_row: pd.Series, legs_df: pd.DataFrame) -> None:
    st.session_state["dashboard_focus_target"] = "parlay_lab"
    st.session_state["parlay_source_session_override"] = "saved_ticket"
    st.session_state["parlay_saved_ticket_id"] = int(ticket_id)
    st.session_state["parlay_saved_ticket_name"] = str(ticket_name)
    st.session_state["parlay_saved_ticket_target"] = str(ticket_row.get("dfs_target_app") or "")
    st.session_state["parlay_saved_ticket_source"] = str(ticket_row.get("source") or "live_edges")
    st.session_state["parlay_saved_ticket_payload"] = legs_df.to_dict(orient="records")

def compact_numeric_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    formatted = df.copy()
    for column in formatted.columns:
        if pd.api.types.is_float_dtype(formatted[column]):
            lowered = str(column).lower()
            if any(token in lowered for token in ["confidence"]):
                decimals = 1
            elif any(token in lowered for token in ["prob", "edge", "line", "projection", "price", "stake", "units", "move", "odds", "value"]):
                decimals = 2
            else:
                decimals = 2
            formatted[column] = formatted[column].map(
                lambda value: ""
                if pd.isna(value)
                else f"{float(value):.{decimals}f}".rstrip("0").rstrip(".")
            )
    return formatted


def annotate_player_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "player" not in df.columns:
        return df

    annotated = df.copy()
    team_source = None
    if "player_team" in annotated.columns:
        team_source = "player_team"
    elif "team" in annotated.columns:
        team_source = "team"

    if not team_source:
        annotated["player_display"] = annotated["player"]
        return annotated

    def _build_label(row: pd.Series) -> str:
        player = str(row.get("player") or "").strip()
        team = str(row.get(team_source) or "").strip()
        if not player:
            return ""
        if not team:
            return player
        if team.lower() in player.lower():
            return player
        return f"{player} ({team})"

    annotated["player_display"] = annotated.apply(_build_label, axis=1)
    return annotated


def prefer_player_display(df: pd.DataFrame, target_column: str = "player") -> pd.DataFrame:
    if df.empty or "player_display" not in df.columns:
        return df
    preferred = df.copy()
    preferred[target_column] = preferred["player_display"]
    return preferred


def prettify_market_label(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    market_map = {
        "player_points": "Points",
        "player_rebounds": "Rebounds",
        "player_assists": "Assists",
        "player_points_rebounds_assists": "PRA",
        "player_threes": "3PT Made",
        "player_first_basket": "First Basket",
        "player_home_runs": "Home Runs",
        "player_hits": "Hits",
        "player_total_bases": "Total Bases",
        "player_strikeouts": "Strikeouts",
        "player_pass_yds": "Pass Yards",
        "player_rush_yds": "Rush Yards",
        "player_reception_yds": "Receiving Yards",
        "player_receptions": "Receptions",
        "Points": "Points",
        "Rebounds": "Rebounds",
        "Assists": "Assists",
        "PRA": "PRA",
        "First Basket": "First Basket",
        "Home Run": "Home Runs",
        "Pitcher Strikeouts": "Strikeouts",
        "Total Bases": "Total Bases",
        "Hits": "Hits",
    }
    if raw in market_map:
        return market_map[raw]
    return raw.replace("_", " ").title()


def prettify_table_headers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    header_map = {
        "leg_rank": "Leg",
        "summary": "Summary",
        "leg_summary": "Summary",
        "player": "Player",
        "player_team": "Team",
        "sport": "Sport",
        "market": "Market",
        "pick": "Pick",
        "line": "Line",
        "projection": "Projection",
        "predicted_value": "Projection",
        "confidence": "Confidence",
        "model_prob": "Model %",
        "win_probability": "Model %",
        "sportsbook": "Sportsbook",
        "commence_time": "Start",
        "last_update": "Updated",
        "coverage_status": "Coverage",
        "recommended_units": "Units",
        "recommended_stake": "Stake",
        "implied_prob": "Implied %",
        "edge": "Edge %",
        "team": "Team",
        "opponent": "Opponent",
    }
    return df.rename(columns={col: header_map.get(col, col.replace("_", " ").title()) for col in df.columns})


def format_bet_label(row: pd.Series) -> str:
    pick = str(row.get("pick") or "").strip()
    line = row.get("line")
    market = prettify_market_label(row.get("market"))
    if pd.isna(line) or line in (None, ""):
        return f"{pick} {market}".strip()
    line_text = f"{float(line):.1f}".rstrip("0").rstrip(".")
    return f"{pick} {line_text} {market}".strip()


def format_live_board_timestamp(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    raw = str(value).strip()
    if not raw or raw.lower() == "none":
        return "-"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return raw
    if getattr(parsed, "tzinfo", None) is not None:
        parsed = parsed.tz_convert(None)
    return parsed.strftime("%b %d, %I:%M %p").replace(" 0", " ")


def format_live_board_price(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    raw = str(value).strip()
    if not raw or raw.lower() == "none":
        return "-"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return raw
    if numeric.is_integer():
        odds = int(numeric)
        return f"+{odds}" if odds > 0 else str(odds)
    return f"{numeric:.2f}".rstrip("0").rstrip(".")


def clean_live_board_display_values(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    for timestamp_col in ["commence_time", "last_update", "pulled_at"]:
        if timestamp_col in cleaned.columns:
            cleaned[timestamp_col] = cleaned[timestamp_col].map(format_live_board_timestamp)
    if "price" in cleaned.columns:
        cleaned["price"] = cleaned["price"].map(format_live_board_price)
    if "coverage_status" in cleaned.columns:
        cleaned["coverage_status"] = (
            cleaned["coverage_status"]
            .fillna("-")
            .astype(str)
            .str.replace("_", " ")
            .str.title()
        )
    if "watchlist" in cleaned.columns:
        cleaned["watchlist"] = cleaned["watchlist"].replace("", "-").fillna("-")
    return cleaned.replace({"None": "-", "nan": "-", "NaT": "-"})


def build_clean_live_board_display(df: pd.DataFrame) -> pd.DataFrame:
    display = prefer_player_display(annotate_player_display(df.copy()))
    if "market" in display.columns:
        display["market"] = display["market"].map(prettify_market_label)
    display["bet"] = display.apply(format_bet_label, axis=1)
    ordered_columns = [
        "sportsbook",
        "player",
        "player_team",
        "bet",
        "price",
        "commence_time",
        "last_update",
        "coverage_status",
        "watchlist",
    ]
    fallback_columns = [col for col in ordered_columns if col in display.columns]
    cleaned = clean_live_board_display_values(display[fallback_columns].copy())
    return prettify_table_headers(cleaned)


def build_clean_edge_display(df: pd.DataFrame) -> pd.DataFrame:
    display = prefer_player_display(annotate_player_display(df.copy()))
    if "market" in display.columns:
        display["market"] = display["market"].map(prettify_market_label)
    display["bet"] = display.apply(format_bet_label, axis=1)
    ordered_columns = [
        "player",
        "player_team",
        "bet",
        "sportsbook",
        "projection",
        "model_prob",
        "implied_prob",
        "edge",
        "confidence",
        "recommended_units",
        "recommended_stake",
        "coverage_status",
        "watchlist",
    ]
    fallback_columns = [col for col in ordered_columns if col in display.columns]
    cleaned = display[fallback_columns].copy()
    return prettify_table_headers(cleaned)


def build_expanded_live_board_display(df: pd.DataFrame) -> pd.DataFrame:
    display = prefer_player_display(annotate_player_display(df.copy()))
    if "market" in display.columns:
        display["market"] = display["market"].map(prettify_market_label)
    display["bet"] = display.apply(format_bet_label, axis=1)
    ordered_columns = [
        "event_id",
        "sportsbook",
        "book_key",
        "player",
        "player_team",
        "market",
        "pick",
        "line",
        "bet",
        "price",
        "side",
        "commence_time",
        "last_update",
        "pulled_at",
        "coverage_status",
        "watchlist",
    ]
    fallback_columns = [col for col in ordered_columns if col in display.columns]
    return prettify_table_headers(clean_live_board_display_values(display[fallback_columns].copy()))


def build_expanded_edge_display(df: pd.DataFrame) -> pd.DataFrame:
    display = prefer_player_display(annotate_player_display(df.copy()))
    if "market" in display.columns:
        display["market"] = display["market"].map(prettify_market_label)
    display["bet"] = display.apply(format_bet_label, axis=1)
    ordered_columns = [
        "player",
        "player_team",
        "market",
        "pick",
        "line",
        "bet",
        "sportsbook",
        "projection",
        "model_prob",
        "implied_prob",
        "edge",
        "confidence",
        "recommended_units",
        "recommended_stake",
        "coverage_status",
        "watchlist",
        "pulled_at",
        "last_update",
    ]
    fallback_columns = [col for col in ordered_columns if col in display.columns]
    return prettify_table_headers(display[fallback_columns].copy())


def build_smart_pick_display(df: pd.DataFrame, top_n: int = 8) -> pd.DataFrame:
    if df.empty:
        return df

    display = prefer_player_display(annotate_player_display(df.copy())).head(top_n)
    if "market" in display.columns:
        display["market"] = display["market"].map(prettify_market_label)
    display["bet"] = display.apply(format_bet_label, axis=1)
    if "smart_expected_win_rate" in display.columns:
        display["smart_expected_win_rate"] = (pd.to_numeric(display["smart_expected_win_rate"], errors="coerce") * 100).round(1)
    if "smart_history_hit_rate" in display.columns:
        display["smart_history_hit_rate"] = (pd.to_numeric(display["smart_history_hit_rate"], errors="coerce") * 100).round(1)
    if "edge" in display.columns:
        display["edge"] = (pd.to_numeric(display["edge"], errors="coerce") * 100).round(1)

    ordered_columns = [
        "player",
        "player_team",
        "bet",
        "sportsbook",
        "smart_tier",
        "smart_score",
        "smart_expected_win_rate",
        "smart_history_hit_rate",
        "edge",
        "confidence",
        "smart_summary",
    ]
    fallback_columns = [col for col in ordered_columns if col in display.columns]
    cleaned = display[fallback_columns].copy()
    cleaned = prettify_table_headers(cleaned)
    return cleaned.rename(
        columns={
            "smart_tier": "Smart Tier",
            "smart_score": "Smart Score",
            "smart_expected_win_rate": "Expected Win %",
            "smart_history_hit_rate": "History Hit %",
            "smart_summary": "Why It Surfaced",
        }
    )


def render_smart_pick_section(
    scored_df: pd.DataFrame,
    history_summary: dict[str, float | int],
    title: str,
    body: str,
    top_n: int = 8,
) -> None:
    st.markdown(f"### {title}")
    st.caption(body)

    history_picks = int(history_summary.get("history_picks", 0) or 0)
    overall_hit_rate = float(history_summary.get("overall_hit_rate", 0.0) or 0.0)
    overall_roi = float(history_summary.get("overall_roi_per_pick", 0.0) or 0.0)

    summary_col1, summary_col2, summary_col3 = st.columns(3)
    summary_col1.metric("History picks", f"{history_picks}")
    summary_col2.metric("Historical hit rate", f"{overall_hit_rate * 100:.1f}%")
    summary_col3.metric("Units per pick", f"{overall_roi:+.2f}")

    if scored_df.empty:
        render_empty_state(
            "No smart picks yet",
            "We need live candidates before the smart scoring engine can rank anything.",
            tone="info",
        )
        return

    smart_display = build_smart_pick_display(scored_df, top_n=top_n)
    st.dataframe(style_signal_table(compact_numeric_table(smart_display)), use_container_width=True, hide_index=True)

    if history_picks <= 0:
        st.info("Smart scoring is currently leaning on live model edge and confidence. Track and grade more picks to unlock stronger historical weighting.")
    elif history_picks < 12:
        st.info("Smart scoring is live, but the history sample is still small. As more graded picks accumulate, market and sportsbook memory will become more reliable.")


def build_smart_learning_display(df: pd.DataFrame, rename_map: dict[str, str], percent_columns: list[str], value_columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    display = df.copy()
    for column in percent_columns:
        if column in display.columns:
            display[column] = (pd.to_numeric(display[column], errors="coerce") * 100).round(1)
    for column in value_columns:
        if column in display.columns:
            display[column] = pd.to_numeric(display[column], errors="coerce").round(2)
    return compact_numeric_table(display.rename(columns=rename_map))


def format_source_label(value: str) -> str:
    label_map = {
        "smart_pick_engine": "Smart Pick Engine",
        "smart_pick_engine_auto": "Smart Pick Engine (Auto)",
        "smart_pick_engine_manual": "Smart Pick Engine (Manual)",
        "edge_scanner": "Edge Scanner",
        "manual_track": "Manual Track",
        "manual_result": "Manual Result",
        "sportsgameodds_auto_settle": "SportsGameOdds Auto-Settle",
    }
    raw = str(value or "").strip()
    if raw in label_map:
        return label_map[raw]
    return raw.replace("_", " ").title()


def format_bool_build_setting(value, true_label: str = "Allowed", false_label: str = "Blocked") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    return true_label if bool(value) else false_label


def active_smart_tracking_source() -> str:
    return "smart_pick_engine_manual" if manual_smart_weight_overrides else "smart_pick_engine_auto"


def build_override_recommendation(source_summary_df: pd.DataFrame) -> tuple[str, str]:
    if source_summary_df.empty:
        return ("Not enough data yet", "Keep collecting graded smart picks before switching modes. The app needs settled results to recommend whether manual overrides are helping.")

    auto_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_auto"].head(1)
    manual_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_manual"].head(1)
    if auto_row.empty or manual_row.empty:
        return ("Need both lanes", "Track and grade picks under both auto and manual smart modes so the app can recommend whether to stay manual or switch back to auto.")

    auto_row = auto_row.iloc[0]
    manual_row = manual_row.iloc[0]
    auto_picks = int(auto_row.get("picks", 0) or 0)
    manual_picks = int(manual_row.get("picks", 0) or 0)
    if auto_picks < 8 or manual_picks < 8:
        return ("Keep testing", f"Auto has {auto_picks} graded picks and manual has {manual_picks}. Let both modes build a larger sample before making a strong call.")

    hit_lift = float(manual_row.get("hit_rate", 0.0) or 0.0) - float(auto_row.get("hit_rate", 0.0) or 0.0)
    roi_lift = float(manual_row.get("roi_per_pick", 0.0) or 0.0) - float(auto_row.get("roi_per_pick", 0.0) or 0.0)
    if roi_lift < -0.08 and hit_lift <= 0:
        return ("Switch back to auto", f"Manual overrides are underperforming auto by {roi_lift:+.2f} units per pick and {hit_lift * 100:+.1f} hit-rate points across a usable sample.")
    if roi_lift > 0.08 or hit_lift > 0.04:
        return ("Stay manual", f"Manual overrides are ahead by {roi_lift:+.2f} units per pick and {hit_lift * 100:+.1f} hit-rate points across the current sample.")
    return ("Too close to call", f"Manual and auto are performing similarly right now: {roi_lift:+.2f} units per pick and {hit_lift * 100:+.1f} hit-rate points apart.")


def apply_recommended_smart_mode(recommendation_title: str, auto_weight_profile: dict[str, float | int | str]) -> bool:
    normalized = str(recommendation_title or "").strip().lower()
    if normalized == "switch back to auto":
        st.session_state["smart_weights_override_enabled"] = False
        persist_preference_if_changed("__app__", "smart_weights_override_enabled", False, False)
        return True
    if normalized == "stay manual":
        st.session_state["smart_weights_override_enabled"] = True
        persist_preference_if_changed("__app__", "smart_weights_override_enabled", True, False)
        defaults = {
            "smart_model_weight": float(auto_weight_profile.get("model_score_weight", 0.42) or 0.42),
            "smart_confidence_weight": float(auto_weight_profile.get("confidence_score_weight", 0.28) or 0.28),
            "smart_edge_multiplier": float(auto_weight_profile.get("edge_multiplier", 1.45) or 1.45),
            "smart_history_market_weight": float(auto_weight_profile.get("history_market_weight", 0.36) or 0.36),
        }
        for session_key, value in defaults.items():
            if session_key not in st.session_state:
                st.session_state[session_key] = value
        return True
    return False


def build_experiment_snapshot_payload(
    graded_df: pd.DataFrame,
    source_summary_df: pd.DataFrame,
    auto_weight_profile: dict[str, float | int | str],
    resolved_weight_profile: dict[str, float | int | str],
    recommendation_title: str,
    recommendation_body: str,
) -> str:
    snapshot = build_experiment_snapshot(graded_df, source_summary_df, rolling_window=10)
    snapshot.update(
        {
            "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
            "recommendation_title": recommendation_title,
            "recommendation_body": recommendation_body,
            "auto_weight_profile": auto_weight_profile,
            "resolved_weight_profile": resolved_weight_profile,
            "manual_override_enabled": bool(st.session_state.get("smart_weights_override_enabled", False)),
        }
    )
    return json.dumps(snapshot, indent=2, default=str)


def render_period_model_review(review: dict[str, object], title: str) -> None:
    st.markdown(f"### {title}")
    current_summary = dict(review.get("current_summary") or {})
    prior_summary = dict(review.get("prior_summary") or {})
    current_label = str(review.get("current_window_label") or "Last 7 days")
    prior_label = str(review.get("prior_window_label") or "Prior 7 days")

    if int(current_summary.get("picks", 0) or 0) <= 0 and int(prior_summary.get("picks", 0) or 0) <= 0:
        render_empty_state(
            "No weekly review yet",
            "Once the app has settled picks across the current and prior review windows, this review will highlight what improved, what slipped, and where the model is currently strongest.",
            tone="info",
        )
        return

    headline = (
        f"{current_label} produced {float(current_summary.get('profit_units', 0.0) or 0.0):+.2f} units "
        f"across {int(current_summary.get('picks', 0) or 0)} graded picks."
    )
    st.info(headline)

    review_col1, review_col2, review_col3, review_col4 = st.columns(4)
    review_col1.metric(
        "Weekly Units",
        f"{float(current_summary.get('profit_units', 0.0) or 0.0):+.2f}u",
        delta=f"{float(current_summary.get('profit_units', 0.0) or 0.0) - float(prior_summary.get('profit_units', 0.0) or 0.0):+.2f}u vs prior",
    )
    review_col2.metric(
        "Weekly Hit Rate",
        f"{float(current_summary.get('hit_rate', 0.0) or 0.0) * 100:.1f}%",
        delta=f"{(float(current_summary.get('hit_rate', 0.0) or 0.0) - float(prior_summary.get('hit_rate', 0.0) or 0.0)) * 100:+.1f} pts",
    )
    review_col3.metric(
        "Units Per Pick",
        f"{float(current_summary.get('units_per_pick', 0.0) or 0.0):+.2f}",
        delta=f"{float(current_summary.get('units_per_pick', 0.0) or 0.0) - float(prior_summary.get('units_per_pick', 0.0) or 0.0):+.2f}",
    )
    review_col4.metric(
        "Graded Picks",
        f"{int(current_summary.get('picks', 0) or 0)}",
        delta=f"{int(current_summary.get('picks', 0) or 0) - int(prior_summary.get('picks', 0) or 0):+d}",
    )

    context_col1, context_col2 = st.columns(2)
    context_col1.caption(
        f"{current_label} leader: {format_source_label(str(current_summary.get('top_source') or 'N/A'))}. "
        f"Best market pocket: {prettify_market_label(str(current_summary.get('top_market') or 'N/A'))}."
    )
    context_col2.caption(
        f"{prior_label} finished at {float(prior_summary.get('profit_units', 0.0) or 0.0):+.2f} units "
        f"with a {float(prior_summary.get('hit_rate', 0.0) or 0.0) * 100:.1f}% hit rate."
    )

    insights = list(review.get("insights") or [])
    if insights:
        st.markdown("#### Weekly Takeaways")
        for insight in insights:
            st.write(f"- {insight}")

    review_tab1, review_tab2 = st.tabs(["By Source", "By Market"])

    with review_tab1:
        source_breakdown = review.get("source_breakdown", pd.DataFrame())
        if isinstance(source_breakdown, pd.DataFrame) and not source_breakdown.empty:
            source_display = source_breakdown.copy()
            source_display["source"] = source_display["source"].map(format_source_label)
            source_display["hit_rate"] = (pd.to_numeric(source_display["hit_rate"], errors="coerce") * 100).round(1)
            source_display["profit_units"] = pd.to_numeric(source_display["profit_units"], errors="coerce").round(2)
            source_display["units_per_pick"] = pd.to_numeric(source_display["units_per_pick"], errors="coerce").round(2)
            source_display = source_display.rename(
                columns={
                    "source": "Workflow Source",
                    "picks": "Tracked Picks",
                    "hit_rate": "Hit Rate %",
                    "profit_units": "Profit Units",
                    "units_per_pick": "Units Per Pick",
                }
            )
            st.dataframe(compact_numeric_table(source_display), use_container_width=True, hide_index=True)
        else:
            st.caption("No weekly source breakdown is available yet.")

    with review_tab2:
        market_breakdown = review.get("market_breakdown", pd.DataFrame())
        if isinstance(market_breakdown, pd.DataFrame) and not market_breakdown.empty:
            market_display = market_breakdown.copy()
            market_display["market"] = market_display["market"].map(prettify_market_label)
            market_display["hit_rate"] = (pd.to_numeric(market_display["hit_rate"], errors="coerce") * 100).round(1)
            market_display["profit_units"] = pd.to_numeric(market_display["profit_units"], errors="coerce").round(2)
            market_display["units_per_pick"] = pd.to_numeric(market_display["units_per_pick"], errors="coerce").round(2)
            market_display = market_display.rename(
                columns={
                    "market": "Market",
                    "picks": "Tracked Picks",
                    "hit_rate": "Hit Rate %",
                    "profit_units": "Profit Units",
                    "units_per_pick": "Units Per Pick",
                }
            )
            st.dataframe(compact_numeric_table(market_display.head(12)), use_container_width=True, hide_index=True)
        else:
            st.caption("No weekly market breakdown is available yet.")


def handle_recommendation_card_action(card: dict[str, str]) -> None:
    action_target = str(card.get("action_target") or "").strip()
    action_section_target = str(card.get("action_section_target") or "").strip()
    if not action_target:
        return
    set_dashboard_focus(action_target)
    if action_target in {"results_grading", "bankroll_journal"} and action_section_target:
        set_results_grading_focus(action_section_target)
    elif action_target == "bankroll_journal":
        set_results_grading_focus("bankroll_journal")
    elif action_target == "parlay_lab" and action_section_target:
        set_parlay_lab_focus(action_section_target)
    elif action_target == "backtest" and action_section_target:
        set_backtest_focus(action_section_target)
    st.rerun()


def render_recommendation_cards(cards: list[dict[str, str]], title: str, key_prefix: str = "recommendation_cards") -> None:
    st.markdown(f"### {title}")
    if not cards:
        render_empty_state(
            "No recommendation cards yet",
            "Once enough graded history is available across the recent review windows, the app will turn trend changes into recommendation cards here.",
            tone="info",
        )
        return
    for idx, card in enumerate(cards):
        confidence_text = str(card.get("confidence") or "").strip()
        confidence_markup = ""
        if confidence_text:
            confidence_markup = (
                f'<div style="'
                f'padding:0.22rem 0.7rem;'
                f'border-radius:999px;'
                f'border:1px solid {theme["card_border"]};'
                f'color:{theme["section_subtitle"]};'
                f'font-size:0.78rem;'
                f'font-weight:700;'
                f'letter-spacing:0.03em;'
                f'text-transform:uppercase;'
                f'margin-top:0.55rem;'
                f'display:inline-block;'
                f'">{confidence_text}</div>'
            )
        card_markup = (
            f'<div style="'
            f'background:{theme["card_bg"]};'
            f'border:1px solid {theme["card_border"]};'
            f'border-radius:20px;'
            f'padding:1rem 1.1rem;'
            f'margin:0 0 0.8rem;'
            f'box-shadow:0 10px 24px rgba(8, 15, 28, 0.08);'
            f'">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;gap:1rem;flex-wrap:wrap;">'
            f'<div style="font-size:1.02rem;font-weight:700;color:{theme["heading_text"]};">{card.get("title", "")}</div>'
            f'<div style="'
            f'padding:0.22rem 0.7rem;'
            f'border-radius:999px;'
            f'border:1px solid {theme["card_border"]};'
            f'color:{theme["section_subtitle"]};'
            f'font-size:0.78rem;'
            f'font-weight:700;'
            f'letter-spacing:0.03em;'
            f'text-transform:uppercase;'
            f'">{card.get("status", "")}</div>'
            f'</div>'
            f'{confidence_markup}'
            f'<div style="margin-top:0.55rem;color:{theme["body_text"]};line-height:1.5;">{card.get("body", "")}</div>'
            f'</div>'
        )
        st.markdown(card_markup, unsafe_allow_html=True)
        action_label = str(card.get("action_label") or "").strip()
        if action_label:
            if st.button(action_label, key=f"{key_prefix}_{idx}", use_container_width=True):
                handle_recommendation_card_action(card)


def apply_review_action_checklist(
    sport_label: str,
    checklist: list[dict[str, object]],
    live_legs_session_key: str,
    live_min_conf_session_key: str,
    live_same_player_session_key: str,
    demo_style_session_key: str,
    demo_same_team_session_key: str,
) -> None:
    setting_to_session = {
        "candidate_pool": "parlay_live_candidate_pool",
        "live_legs": live_legs_session_key,
        "live_min_confidence": live_min_conf_session_key,
        "live_same_player": live_same_player_session_key,
        "demo_style": demo_style_session_key,
        "demo_same_team": demo_same_team_session_key,
    }
    setting_to_preference = {
        "candidate_pool": "parlay_source",
        "live_legs": "live_legs",
        "live_min_confidence": "live_min_confidence",
        "live_same_player": "live_same_player",
        "demo_style": "demo_parlay_style",
        "demo_same_team": "demo_same_team",
    }
    for item in checklist:
        setting_key = str(item.get("setting_key") or "")
        if setting_key not in setting_to_session:
            continue
        value = item.get("value")
        session_key = setting_to_session[setting_key]
        st.session_state[session_key] = value
        persist_preference_if_changed(sport_label, setting_to_preference[setting_key], value, value)


def render_review_action_checklist(
    checklist: list[dict[str, object]],
    sport_label: str,
    live_legs_session_key: str,
    live_min_conf_session_key: str,
    live_same_player_session_key: str,
    demo_style_session_key: str,
    demo_same_team_session_key: str,
) -> None:
    st.markdown("### Action Checklist")
    if not checklist:
        render_empty_state(
            "No checklist yet",
            "As review windows fill in, the app will translate current trend posture into settings you can apply straight to Parlay Lab.",
            tone="info",
        )
        return
    for item in checklist:
        st.markdown(
            f"- **{item.get('label', '')}**: set to `{item.get('value')}`. {item.get('reason', '')}"
        )
    if st.button("Apply Checklist To Parlay Lab", use_container_width=True):
        apply_review_action_checklist(
            sport_label=sport_label,
            checklist=checklist,
            live_legs_session_key=live_legs_session_key,
            live_min_conf_session_key=live_min_conf_session_key,
            live_same_player_session_key=live_same_player_session_key,
            demo_style_session_key=demo_style_session_key,
            demo_same_team_session_key=demo_same_team_session_key,
        )
        st.session_state["dashboard_focus_target"] = "parlay_lab"
        st.success("Applied the current review checklist to Parlay Lab for this sport.")
        st.rerun()
        return


def render_smart_parlay_profile_panel(
    profile: dict[str, object],
    title: str,
    mode_label: str,
    current_values: dict[str, object],
    apply_button_key: str,
    apply_callback,
    pending_updates_key: str,
) -> None:
    st.markdown(f"### {title}")
    sample_size = int(profile.get("sample_size", 0) or 0)
    reason = str(profile.get("reason") or "")

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    if "recommended_style" in profile:
        metric_col1.metric("Recommended style", str(profile.get("recommended_style") or "Balanced"))
    else:
        metric_col1.metric("Recommended legs", str(profile.get("recommended_legs") or current_values.get("legs") or 3))
    metric_col2.metric("Recommended legs", str(profile.get("recommended_legs") or current_values.get("legs") or 3))
    metric_col3.metric("Min confidence", str(profile.get("recommended_min_confidence") or current_values.get("min_confidence") or 65))
    exposure_key = "recommended_same_team" if "recommended_same_team" in profile else "recommended_same_player"
    exposure_label = "Same team" if exposure_key == "recommended_same_team" else "Same player"
    exposure_allowed = bool(profile.get(exposure_key, current_values.get("allow_overlap", False)))
    metric_col4.metric(exposure_label, "Allowed" if exposure_allowed else "Blocked")

    if str(profile.get("recommended_target_label") or "").strip():
        st.caption(f"Preferred DFS destination: {profile['recommended_target_label']}")
    if reason:
        st.info(reason)
    if sample_size <= 0:
        st.caption(f"{mode_label} profile is currently using defaults until ticket history grows.")
    else:
        st.caption(f"{mode_label} profile is informed by {sample_size} historical tickets.")

    if st.button("Apply Smart Profile", key=apply_button_key, use_container_width=False):
        pending_updates = apply_callback(profile) or {}
        st.session_state[pending_updates_key] = pending_updates
        st.rerun()


def apply_pending_profile_updates(pending_updates_key: str) -> None:
    pending_updates = st.session_state.pop(pending_updates_key, None)
    if not isinstance(pending_updates, dict) or not pending_updates:
        return
    for session_key, value in pending_updates.items():
        st.session_state[session_key] = value


def get_manual_smart_weight_overrides() -> dict[str, float] | None:
    enabled = bool(st.session_state.get("smart_weights_override_enabled", False))
    if not enabled:
        return None
    return {
        "model_score_weight": float(st.session_state.get("smart_model_weight", 0.42)),
        "confidence_score_weight": float(st.session_state.get("smart_confidence_weight", 0.28)),
        "edge_multiplier": float(st.session_state.get("smart_edge_multiplier", 1.45)),
        "history_market_weight": float(st.session_state.get("smart_history_market_weight", 0.36)),
    }


def build_watchlist_option_labels(df: pd.DataFrame) -> dict[str, int]:
    option_labels = {}
    for idx, row in df.iterrows():
        player = str(row.get("player_display") or row.get("player", "Unknown"))
        market = str(row.get("market", "Unknown"))
        pick = str(row.get("pick", ""))
        sportsbook = str(row.get("sportsbook", row.get("bookmaker_title", "")))
        line = row.get("line")
        line_text = "" if pd.isna(line) else f" @ {line}"
        option_labels[f"{player} | {market} | {pick}{line_text} | {sportsbook}"] = idx
    return option_labels


def promote_watchlist_alerts_to_parlay_lab() -> None:
    st.session_state["parlay_live_candidate_pool"] = "Watchlist alerts"
    st.session_state["parlay_live_use_watchlist_alerts"] = True
    st.session_state["dashboard_focus_target"] = "parlay_lab"


def set_dashboard_focus(target: str) -> None:
    st.session_state["dashboard_focus_target"] = target


def set_results_grading_focus(target: str) -> None:
    st.session_state["results_grading_section_focus_target"] = target


def set_parlay_lab_focus(target: str) -> None:
    st.session_state["parlay_lab_section_focus_target"] = target


def set_backtest_focus(target: str) -> None:
    st.session_state["backtest_section_focus_target"] = target


def sync_view_preference_state(sport_label: str, session_key: str, preference_key: str, default: str) -> None:
    if session_key not in st.session_state:
        st.session_state[session_key] = get_view_preference(sport_label, preference_key, default)


def sync_typed_view_preference_state(
    sport_label: str,
    session_key: str,
    preference_key: str,
    default,
    caster,
) -> None:
    if session_key in st.session_state:
        return
    saved_value = get_view_preference(sport_label, preference_key, str(default))
    try:
        st.session_state[session_key] = caster(saved_value)
    except Exception:
        st.session_state[session_key] = default


def sync_bool_view_preference_state(
    sport_label: str,
    session_key: str,
    preference_key: str,
    default: bool,
) -> None:
    if session_key in st.session_state:
        return
    saved_value = get_view_preference(sport_label, preference_key, str(default))
    st.session_state[session_key] = str(saved_value).strip().lower() == "true"


def persist_view_preference_from_session(sport_label: str, session_key: str, preference_key: str) -> None:
    save_view_preference(sport_label, preference_key, str(st.session_state.get(session_key, "Compact")))


def persist_preference_if_changed(sport_label: str, preference_key: str, value, default) -> None:
    current_saved = get_view_preference(sport_label, preference_key, str(default))
    current_value = str(value)
    if current_saved != current_value:
        save_view_preference(sport_label, preference_key, current_value)

init_db()

theme_session_key = "app_theme_mode"
sync_view_preference_state("__app__", theme_session_key, "theme_mode", "Light")
theme_mode = st.session_state.get(theme_session_key, "Light")
persist_preference_if_changed("__app__", "theme_mode", theme_mode, "Light")

theme_tokens = {
    "Light": {
        "app_bg": "radial-gradient(circle at top left, rgba(244, 162, 97, 0.14), transparent 28%), radial-gradient(circle at top right, rgba(42, 157, 143, 0.12), transparent 26%), linear-gradient(180deg, #f6f4ee 0%, #fcfbf8 100%)",
        "hero_bg": "linear-gradient(135deg, rgba(24, 35, 52, 0.94), rgba(34, 63, 95, 0.92))",
        "hero_border": "rgba(255,255,255,0.08)",
        "hero_brand_bg": "rgba(255,255,255,0.08)",
        "hero_brand_border": "rgba(255,255,255,0.12)",
        "hero_pill_bg": "rgba(255,255,255,0.08)",
        "hero_pill_border": "rgba(255,255,255,0.09)",
        "header_bg": "rgba(252, 251, 248, 0.88)",
        "header_border": "rgba(31, 41, 55, 0.06)",
        "toolbar_text": "#526273",
        "toolbar_icon": "#526273",
        "toolbar_hover_bg": "rgba(38, 70, 83, 0.08)",
        "hero_text": "#f8fafc",
        "hero_subtitle": "rgba(248, 250, 252, 0.84)",
        "eyebrow": "#f4a261",
        "section_title": "#1f2937",
        "section_subtitle": "#6b7280",
        "card_bg": "rgba(255,255,255,0.84)",
        "card_border": "rgba(31, 41, 55, 0.08)",
        "metric_bg": "rgba(255,255,255,0.74)",
        "metric_border": "rgba(31, 41, 55, 0.08)",
        "table_bg": "rgba(255,255,255,0.72)",
        "table_border": "rgba(31, 41, 55, 0.06)",
        "sidebar_bg": "linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248, 246, 241, 0.98))",
        "sidebar_border": "rgba(31, 41, 55, 0.06)",
        "sidebar_text": "#1f2937",
        "sidebar_muted": "#6b7280",
        "tabs_bg": "rgba(255,255,255,0.66)",
        "tabs_border": "rgba(31, 41, 55, 0.06)",
        "tab_text": "#435266",
        "tab_active_bg": "linear-gradient(135deg, #264653, #2a9d8f)",
        "tab_active_text": "white",
        "input_bg": "#f8f5ec",
        "input_border": "#d6d0c2",
        "input_text": "#1f2937",
        "input_label": "#4b5563",
        "top_control_label": "#374151",
        "expander_text": "#334155",
        "body_text": "#334155",
        "heading_text": "#1f2937",
        "button_bg": "linear-gradient(135deg, #ffffff, #f7f2e7)",
        "button_text": "#17324d",
        "button_border": "#d8d2c5",
    },
    "Dark": {
        "app_bg": "radial-gradient(circle at top left, rgba(56, 189, 248, 0.11), transparent 22%), radial-gradient(circle at top right, rgba(99, 102, 241, 0.12), transparent 24%), linear-gradient(180deg, #08111f 0%, #0b1426 48%, #0d1830 100%)",
        "hero_bg": "linear-gradient(135deg, rgba(12, 26, 47, 0.98), rgba(19, 44, 77, 0.96))",
        "hero_border": "rgba(96,165,250,0.22)",
        "hero_brand_bg": "rgba(12, 26, 47, 0.76)",
        "hero_brand_border": "rgba(125, 211, 252, 0.22)",
        "hero_pill_bg": "rgba(10, 22, 40, 0.64)",
        "hero_pill_border": "rgba(96, 165, 250, 0.20)",
        "header_bg": "rgba(8, 17, 31, 0.78)",
        "header_border": "rgba(96, 165, 250, 0.10)",
        "toolbar_text": "#bfd6ef",
        "toolbar_icon": "#d7e8fa",
        "toolbar_hover_bg": "rgba(37, 99, 235, 0.14)",
        "hero_text": "#edf6ff",
        "hero_subtitle": "rgba(208, 225, 244, 0.88)",
        "eyebrow": "#7dd3fc",
        "section_title": "#f3f8ff",
        "section_subtitle": "#9fb4cc",
        "card_bg": "rgba(11, 22, 39, 0.92)",
        "card_border": "rgba(96, 165, 250, 0.16)",
        "metric_bg": "rgba(14, 28, 48, 0.90)",
        "metric_border": "rgba(96, 165, 250, 0.14)",
        "table_bg": "rgba(13, 25, 44, 0.90)",
        "table_border": "rgba(96, 165, 250, 0.12)",
        "sidebar_bg": "linear-gradient(180deg, rgba(7, 16, 30, 0.99), rgba(10, 20, 36, 0.99))",
        "sidebar_border": "rgba(96, 165, 250, 0.12)",
        "sidebar_text": "#deebfb",
        "sidebar_muted": "#8fa7c4",
        "tabs_bg": "rgba(10, 22, 40, 0.76)",
        "tabs_border": "rgba(96, 165, 250, 0.10)",
        "tab_text": "#9fc4e8",
        "tab_active_bg": "linear-gradient(135deg, #2563eb, #0891b2)",
        "tab_active_text": "#f8fafc",
        "input_bg": "rgba(16, 33, 58, 0.96)",
        "input_border": "#315b8f",
        "input_text": "#eaf4ff",
        "input_label": "#c9dcf3",
        "top_control_label": "#deebfb",
        "expander_text": "#d7e8fa",
        "body_text": "#d8e7f7",
        "heading_text": "#f3f8ff",
        "button_bg": "linear-gradient(135deg, #1d4ed8, #0f766e)",
        "button_text": "#f8fbff",
        "button_border": "rgba(125, 211, 252, 0.24)",
    },
}
theme = theme_tokens.get(theme_mode, theme_tokens["Light"])

st.set_page_config(
    page_title="AI Parlay Builder",
    page_icon=str(BRANDMARK_PATH) if BRANDMARK_PATH.exists() else None,
    layout="wide",
)
base_css = """
<style>
.stApp {
    background: __APP_BG__;
}
header[data-testid="stHeader"] {
    background: __HEADER_BG__;
    border-bottom: 1px solid __HEADER_BORDER__;
}
[data-testid="stToolbar"] {
    background: transparent;
}
header[data-testid="stHeader"] a,
header[data-testid="stHeader"] button,
header[data-testid="stHeader"] [role="button"],
[data-testid="stToolbar"] a,
[data-testid="stToolbar"] button {
    color: __TOOLBAR_TEXT__;
}
header[data-testid="stHeader"] svg,
[data-testid="stToolbar"] svg {
    fill: __TOOLBAR_ICON__;
}
header[data-testid="stHeader"] a:hover,
header[data-testid="stHeader"] button:hover,
header[data-testid="stHeader"] [role="button"]:hover,
[data-testid="stToolbar"] a:hover,
[data-testid="stToolbar"] button:hover {
    background: __TOOLBAR_HOVER_BG__;
    border-radius: 10px;
}
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 3rem;
    max-width: 1400px;
}
.top-select-label {
    color: __TOP_CONTROL_LABEL__;
    font-size: 0.95rem;
    font-weight: 700;
    letter-spacing: 0.01em;
    margin-bottom: 0.35rem;
    text-shadow: 0 1px 0 rgba(255, 255, 255, 0.72);
}
.app-hero {
    padding: 1.5rem 1.6rem;
    border-radius: 24px;
    background: __HERO_BG__;
    color: __HERO_TEXT__;
    border: 1px solid __HERO_BORDER__;
    box-shadow: 0 20px 50px rgba(15, 23, 42, 0.18);
    margin-bottom: 1rem;
}
.app-hero__brand {
    display: flex;
    align-items: flex-start;
    gap: 1rem;
}
.app-hero__brandmark {
    width: 68px;
    height: 68px;
    border-radius: 20px;
    flex-shrink: 0;
    box-shadow: 0 10px 24px rgba(15, 23, 42, 0.24);
    border: 1px solid __HERO_BRAND_BORDER__;
    background: __HERO_BRAND_BG__;
}
.app-hero__eyebrow {
    text-transform: uppercase;
    letter-spacing: 0.16em;
    font-size: 0.72rem;
    color: __EYEBROW__;
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
    color: __HERO_SUBTITLE__;
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
    background: __HERO_PILL_BG__;
    border: 1px solid __HERO_PILL_BORDER__;
    font-size: 0.84rem;
}
.priority-strip {
    display: grid;
    grid-template-columns: minmax(260px, 1.15fr) minmax(0, 1.85fr);
    gap: 0.9rem;
    margin: 0.15rem 0 1rem;
}
.priority-strip__mode,
.priority-strip__card {
    background: __CARD_BG__;
    border: 1px solid __CARD_BORDER__;
    border-radius: 20px;
    padding: 1rem 1.05rem;
    box-shadow: 0 10px 24px rgba(8, 15, 28, 0.08);
}
.priority-strip__cards {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.9rem;
}
.priority-strip__eyebrow {
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-size: 0.72rem;
    font-weight: 800;
    color: __EYEBROW__;
    margin-bottom: 0.38rem;
}
.priority-strip__mode-title,
.priority-strip__title {
    color: __HEADING_TEXT__;
    font-weight: 800;
    line-height: 1.2;
}
.priority-strip__mode-title {
    font-size: 1.02rem;
    margin-bottom: 0.42rem;
}
.priority-strip__title {
    font-size: 0.98rem;
    margin-bottom: 0.35rem;
}
.priority-strip__body {
    color: __BODY_TEXT__;
    font-size: 0.9rem;
    line-height: 1.5;
}
.priority-strip__pulse {
    margin-top: 0.75rem;
    padding-top: 0.75rem;
    border-top: 1px solid __CARD_BORDER__;
    display: grid;
    gap: 0.22rem;
}
.priority-strip__pulse strong {
    color: __HEADING_TEXT__;
    font-size: 0.9rem;
}
.priority-strip__pulse span {
    color: __BODY_TEXT__;
    font-size: 0.86rem;
    line-height: 1.45;
}
.priority-strip__badges {
    display: flex;
    flex-wrap: wrap;
    gap: 0.45rem;
    margin-top: 0.75rem;
}
.priority-strip__badge {
    display: inline-block;
    padding: 0.32rem 0.6rem;
    border-radius: 999px;
    background: __HERO_PILL_BG__;
    border: 1px solid __CARD_BORDER__;
    color: __SECTION_SUBTITLE__;
    font-size: 0.76rem;
    font-weight: 700;
}
.priority-strip__badge--good {
    background: rgba(16, 185, 129, 0.12);
    border-color: rgba(16, 185, 129, 0.22);
    color: #0f8a63;
}
.priority-strip__badge--warn {
    background: rgba(245, 158, 11, 0.12);
    border-color: rgba(245, 158, 11, 0.24);
    color: #b7791f;
}
.priority-strip__badge--alert {
    background: rgba(239, 68, 68, 0.12);
    border-color: rgba(239, 68, 68, 0.24);
    color: #c24141;
}
.priority-strip__badge--info {
    background: rgba(59, 130, 246, 0.12);
    border-color: rgba(59, 130, 246, 0.22);
    color: #2563eb;
}
.section-header {
    margin: 0.1rem 0 0.9rem;
}
.section-header__title {
    font-size: 1.35rem;
    font-weight: 800;
    color: __SECTION_TITLE__;
    margin-bottom: 0.15rem;
}
.section-header__subtitle {
    font-size: 0.95rem;
    color: __SECTION_SUBTITLE__;
}
.stApp,
.stApp p,
.stApp li,
.stApp div[data-testid="stMarkdownContainer"] p,
.stApp [data-testid="stCaptionContainer"] {
    color: __BODY_TEXT__;
}
.stApp h1,
.stApp h2,
.stApp h3,
.stApp h4,
.stApp h5,
.stApp h6,
.stMarkdown h1,
.stMarkdown h2,
.stMarkdown h3,
.stMarkdown h4 {
    color: __HEADING_TEXT__;
    letter-spacing: -0.01em;
}
.watchlist-alert-card {
    background: __CARD_BG__;
    border: 1px solid __CARD_BORDER__;
    border-radius: 18px;
    padding: 0.95rem 1rem;
    box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
    margin-bottom: 0.75rem;
}
.watchlist-alert-card__title {
    font-size: 0.98rem;
    font-weight: 800;
    color: __SECTION_TITLE__;
    margin-bottom: 0.18rem;
}
.watchlist-alert-card__subtitle {
    font-size: 0.86rem;
    color: __SECTION_SUBTITLE__;
    margin-bottom: 0.7rem;
}
.watchlist-alert-card__metrics {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.55rem;
    margin-bottom: 0.7rem;
}
.watchlist-alert-card__metrics span {
    display: block;
    font-size: 0.72rem;
    color: __SECTION_SUBTITLE__;
    text-transform: uppercase;
    margin-bottom: 0.1rem;
}
.watchlist-alert-card__metrics strong {
    font-size: 0.98rem;
    color: __SECTION_TITLE__;
}
.watchlist-alert-card__signals {
    display: flex;
    gap: 0.45rem;
    flex-wrap: wrap;
}
.watchlist-alert-card__signals span {
    display: inline-block;
    padding: 0.28rem 0.55rem;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 700;
}
.watchlist-alert-card__freshness {
    margin-top: 0.65rem;
    font-size: 0.8rem;
    color: __SECTION_SUBTITLE__;
}
[data-testid="stMetric"] {
    background: __METRIC_BG__;
    border: 1px solid __METRIC_BORDER__;
    padding: 0.9rem 1rem;
    border-radius: 18px;
    box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
}
[data-testid="stMetricLabel"] {
    white-space: normal;
}
[data-testid="stMetricValue"] {
    font-size: clamp(2rem, 2vw, 3.1rem);
    line-height: 1.08;
}
[data-testid="stMetricValue"] > div {
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: unset !important;
    word-break: break-word;
}
@media (max-width: 1200px) {
    [data-testid="stMetric"] {
        padding: 0.8rem 0.85rem;
    }
    [data-testid="stMetricValue"] {
        font-size: clamp(1.7rem, 3vw, 2.6rem);
    }
}
@media (max-width: 860px) {
    .priority-strip {
        grid-template-columns: 1fr;
    }
    .priority-strip__cards {
        grid-template-columns: 1fr;
    }
    [data-testid="stMetricValue"] {
        font-size: clamp(1.45rem, 4.6vw, 2.1rem);
    }
    .app-hero {
        padding: 1rem 1rem 1.1rem;
    }
    .app-hero__title {
        font-size: clamp(1.85rem, 7vw, 2.55rem);
    }
    .app-hero__subtitle {
        font-size: 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        flex-wrap: wrap;
    }
    .stTabs [data-baseweb="tab"] {
        flex: 1 1 calc(50% - 0.35rem);
        justify-content: center;
        min-height: 44px;
        text-align: center;
    }
    [data-testid="stDataFrame"] div[role="grid"] {
        font-size: 0.84rem;
    }
    .stButton > button {
        min-height: 44px;
    }
}
@media (max-width: 640px) {
    .priority-strip__mode,
    .priority-strip__card {
        padding: 0.9rem 0.95rem;
    }
    .stTabs [data-baseweb="tab"] {
        flex: 1 1 100%;
    }
}
[data-testid="stDataFrame"] {
    background: __TABLE_BG__;
    border-radius: 18px;
    padding: 0.15rem;
    border: 1px solid __TABLE_BORDER__;
}
[data-testid="stDataFrame"] div[role="grid"] {
    font-size: 0.92rem;
}
[data-testid="stDataFrame"] [role="columnheader"] {
    letter-spacing: 0.03em;
    font-weight: 700;
}
[data-testid="stSidebar"] {
    background: __SIDEBAR_BG__;
    border-right: 1px solid __SIDEBAR_BORDER__;
}
[data-testid="stSidebar"],
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] li,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
    color: __SIDEBAR_TEXT__;
}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] small {
    color: __SIDEBAR_MUTED__;
}
.stTabs [data-baseweb="tab-list"] {
    gap: 0.35rem;
    background: __TABS_BG__;
    padding: 0.45rem;
    border-radius: 18px;
    border: 1px solid __TABS_BORDER__;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 14px;
    padding: 0.45rem 0.9rem;
    font-weight: 700;
    color: __TAB_TEXT__;
}
.stTabs [aria-selected="true"] {
    background: __TAB_ACTIVE_BG__;
    color: __TAB_ACTIVE_TEXT__;
}
.stTabs [data-baseweb="tab"]:hover {
    color: __SECTION_TITLE__;
}
[data-baseweb="select"] > div,
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stTextArea"] textarea {
    background: __INPUT_BG__;
    color: __INPUT_TEXT__;
    border-color: __INPUT_BORDER__;
}
[data-baseweb="select"] svg,
[data-testid="stTextInput"] svg,
[data-testid="stNumberInput"] svg {
    fill: __INPUT_TEXT__;
}
[data-baseweb="select"] [data-testid="stMarkdownContainer"] p {
    color: __INPUT_TEXT__;
}
label,
[data-testid="stWidgetLabel"],
.stRadio label,
.stCheckbox label {
    color: __INPUT_LABEL__;
    font-weight: 600;
    opacity: 0.98;
    text-shadow: 0 1px 0 rgba(255, 255, 255, 0.45);
}
[data-testid="stWidgetLabel"] p,
label p {
    color: __INPUT_LABEL__ !important;
}
details summary,
[data-testid="stExpander"] summary,
[data-testid="stExpander"] details summary p {
    color: __EXPANDER_TEXT__;
}
.stButton > button {
    background: __BUTTON_BG__;
    color: __BUTTON_TEXT__;
    border: 1px solid __BUTTON_BORDER__;
    box-shadow: 0 10px 24px rgba(8, 15, 28, 0.18);
}
.stButton > button:hover {
    filter: brightness(1.05);
    border-color: __BUTTON_BORDER__;
}
</style>
"""
theme_css = (
    base_css.replace("__APP_BG__", theme["app_bg"])
    .replace("__HEADER_BG__", theme["header_bg"])
    .replace("__HEADER_BORDER__", theme["header_border"])
    .replace("__TOOLBAR_TEXT__", theme["toolbar_text"])
    .replace("__TOOLBAR_ICON__", theme["toolbar_icon"])
    .replace("__TOOLBAR_HOVER_BG__", theme["toolbar_hover_bg"])
    .replace("__HERO_BG__", theme["hero_bg"])
    .replace("__HERO_BRAND_BG__", theme["hero_brand_bg"])
    .replace("__HERO_BRAND_BORDER__", theme["hero_brand_border"])
    .replace("__HERO_TEXT__", theme["hero_text"])
    .replace("__HERO_BORDER__", theme["hero_border"])
    .replace("__HERO_PILL_BG__", theme["hero_pill_bg"])
    .replace("__HERO_PILL_BORDER__", theme["hero_pill_border"])
    .replace("__EYEBROW__", theme["eyebrow"])
    .replace("__HERO_SUBTITLE__", theme["hero_subtitle"])
    .replace("__SECTION_TITLE__", theme["section_title"])
    .replace("__SECTION_SUBTITLE__", theme["section_subtitle"])
    .replace("__CARD_BG__", theme["card_bg"])
    .replace("__CARD_BORDER__", theme["card_border"])
    .replace("__METRIC_BG__", theme["metric_bg"])
    .replace("__METRIC_BORDER__", theme["metric_border"])
    .replace("__TABLE_BG__", theme["table_bg"])
    .replace("__TABLE_BORDER__", theme["table_border"])
    .replace("__SIDEBAR_BG__", theme["sidebar_bg"])
    .replace("__SIDEBAR_BORDER__", theme["sidebar_border"])
    .replace("__SIDEBAR_TEXT__", theme["sidebar_text"])
    .replace("__SIDEBAR_MUTED__", theme["sidebar_muted"])
    .replace("__TABS_BG__", theme["tabs_bg"])
    .replace("__TABS_BORDER__", theme["tabs_border"])
    .replace("__TAB_TEXT__", theme["tab_text"])
    .replace("__TAB_ACTIVE_BG__", theme["tab_active_bg"])
    .replace("__TAB_ACTIVE_TEXT__", theme["tab_active_text"])
    .replace("__INPUT_BG__", theme["input_bg"])
    .replace("__INPUT_BORDER__", theme["input_border"])
    .replace("__INPUT_TEXT__", theme["input_text"])
    .replace("__INPUT_LABEL__", theme["input_label"])
    .replace("__TOP_CONTROL_LABEL__", theme["top_control_label"])
    .replace("__EXPANDER_TEXT__", theme["expander_text"])
    .replace("__BODY_TEXT__", theme["body_text"])
    .replace("__HEADING_TEXT__", theme["heading_text"])
    .replace("__BUTTON_BG__", theme["button_bg"])
    .replace("__BUTTON_TEXT__", theme["button_text"])
    .replace("__BUTTON_BORDER__", theme["button_border"])
)
st.markdown(theme_css, unsafe_allow_html=True)

selector_col1, selector_col2, selector_col3 = st.columns([1.1, 0.95, 0.8])
sport_labels = get_sport_labels()
app_sport_session_key = "selected_sport_label"
sync_view_preference_state("__app__", app_sport_session_key, "selected_sport_label", sport_labels[0])
sync_bool_view_preference_state("__app__", "smart_weights_override_enabled", "smart_weights_override_enabled", False)
sync_bool_view_preference_state("__app__", "top_priority_strip_collapsed", "top_priority_strip_collapsed", False)
sync_typed_view_preference_state("__app__", "smart_model_weight", "smart_model_weight", 0.42, float)
sync_typed_view_preference_state("__app__", "smart_confidence_weight", "smart_confidence_weight", 0.28, float)
sync_typed_view_preference_state("__app__", "smart_edge_multiplier", "smart_edge_multiplier", 1.45, float)
sync_typed_view_preference_state("__app__", "smart_history_market_weight", "smart_history_market_weight", 0.36, float)
manual_smart_weight_overrides = get_manual_smart_weight_overrides()
with selector_col1:
    st.markdown('<div class="top-select-label">Sport</div>', unsafe_allow_html=True)
    sport_label = st.selectbox(
        "Sport",
        sport_labels,
        key=app_sport_session_key,
        label_visibility="collapsed",
        on_change=persist_view_preference_from_session,
        args=("__app__", app_sport_session_key, "selected_sport_label"),
    )
persist_preference_if_changed("__app__", "selected_sport_label", sport_label, sport_labels[0])
board_type_session_key = f"board_type_{sport_label}"
parlay_source_session_key = f"parlay_source_{sport_label}"
sync_view_preference_state(sport_label, board_type_session_key, "board_type", "Sportsbook")
sync_view_preference_state(sport_label, parlay_source_session_key, "parlay_source", "Live edges")
with selector_col2:
    st.markdown('<div class="top-select-label">Board Type</div>', unsafe_allow_html=True)
    board_type = st.selectbox(
        "Board Type",
        ["Sportsbook", "DFS"],
        key=board_type_session_key,
        label_visibility="collapsed",
        on_change=persist_view_preference_from_session,
        args=(sport_label, board_type_session_key, "board_type"),
    )
persist_preference_if_changed(sport_label, "board_type", board_type, "Sportsbook")
with selector_col3:
    st.markdown('<div class="top-select-label">Theme</div>', unsafe_allow_html=True)
    theme_mode = st.selectbox(
        "Theme",
        ["Light", "Dark"],
        key=theme_session_key,
        label_visibility="collapsed",
        on_change=persist_view_preference_from_session,
        args=("__app__", theme_session_key, "theme_mode"),
    )
persist_preference_if_changed("__app__", "theme_mode", theme_mode, "Light")

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
watchlist_df = get_watchlist_df(sport_label)
watchlist_df = annotate_player_display(watchlist_df) if not watchlist_df.empty else watchlist_df
watchlist_alert_settings = get_watchlist_alert_settings(sport_label)
board_view_session_key = f"board_view_mode_{sport_label}"
edge_view_session_key = f"edge_view_mode_{sport_label}"
parlay_view_session_key = f"parlay_view_mode_{sport_label}"
demo_parlay_view_session_key = f"demo_parlay_view_mode_{sport_label}"
show_non_live_board_session_key = f"show_non_live_board_{sport_label}"
show_non_live_edges_session_key = f"show_non_live_edges_{sport_label}"
history_player_suggestion_session_key = f"history_player_suggestion_{sport_label}"
history_market_suggestion_session_key = f"history_market_suggestion_{sport_label}"
graded_market_filter_session_key = f"graded_market_filter_{sport_label}"
graded_sort_by_session_key = f"graded_sort_by_{sport_label}"
board_market_filter_session_key = f"board_market_filter_{sport_label}"
board_sort_by_session_key = f"board_sort_by_{sport_label}"
board_sort_ascending_session_key = f"board_sort_ascending_{sport_label}"
board_watchlist_only_session_key = f"board_watchlist_only_{sport_label}"
edge_market_filter_session_key = f"edge_market_filter_{sport_label}"
edge_sort_by_session_key = f"edge_sort_by_{sport_label}"
edge_sort_ascending_session_key = f"edge_sort_ascending_{sport_label}"
edge_watchlist_only_session_key = f"edge_watchlist_only_{sport_label}"
edge_alerts_only_session_key = f"edge_alerts_only_{sport_label}"
live_min_conf_session_key = f"live_min_conf_{sport_label}"
live_legs_session_key = f"live_legs_{sport_label}"
live_same_player_session_key = f"live_same_player_{sport_label}"
demo_legs_session_key = f"demo_legs_{sport_label}"
demo_min_conf_session_key = f"demo_min_conf_{sport_label}"
demo_style_session_key = f"demo_style_{sport_label}"
demo_same_team_session_key = f"demo_same_team_{sport_label}"
live_profile_pending_updates_key = f"live_profile_pending_updates_{sport_label}"
demo_profile_pending_updates_key = f"demo_profile_pending_updates_{sport_label}"
apply_pending_profile_updates(live_profile_pending_updates_key)
apply_pending_profile_updates(demo_profile_pending_updates_key)
sync_view_preference_state(sport_label, board_view_session_key, "board_view_mode", "Compact")
sync_view_preference_state(sport_label, edge_view_session_key, "edge_view_mode", "Compact")
sync_view_preference_state(sport_label, parlay_view_session_key, "parlay_view_mode", "Compact")
sync_view_preference_state(sport_label, demo_parlay_view_session_key, "demo_parlay_view_mode", "Compact")
sync_bool_view_preference_state(sport_label, show_non_live_board_session_key, "show_non_live_board", not sync_enabled)
sync_bool_view_preference_state(sport_label, show_non_live_edges_session_key, "show_non_live_edges", False)
sync_view_preference_state(sport_label, history_player_suggestion_session_key, "history_player_suggestion", "")
sync_view_preference_state(sport_label, history_market_suggestion_session_key, "history_market_suggestion", "")
sync_view_preference_state(sport_label, graded_market_filter_session_key, "graded_market_filter", "")
sync_view_preference_state(sport_label, graded_sort_by_session_key, "graded_sort_by", "resolved_at")
sync_view_preference_state(sport_label, board_market_filter_session_key, "board_market_filter", "")
sync_view_preference_state(sport_label, board_sort_by_session_key, "board_sort_by", "pulled_at")
sync_bool_view_preference_state(sport_label, board_sort_ascending_session_key, "board_sort_ascending", False)
sync_bool_view_preference_state(sport_label, board_watchlist_only_session_key, "board_watchlist_only", False)
sync_view_preference_state(sport_label, edge_market_filter_session_key, "edge_market_filter", "")
sync_view_preference_state(sport_label, edge_sort_by_session_key, "edge_sort_by", "confidence")
sync_bool_view_preference_state(sport_label, edge_sort_ascending_session_key, "edge_sort_ascending", False)
sync_bool_view_preference_state(sport_label, edge_watchlist_only_session_key, "edge_watchlist_only", False)
sync_bool_view_preference_state(sport_label, edge_alerts_only_session_key, "edge_alerts_only", False)
sync_typed_view_preference_state(sport_label, live_legs_session_key, "live_legs", 3, int)
sync_typed_view_preference_state(sport_label, live_min_conf_session_key, "live_min_confidence", 65, int)
sync_bool_view_preference_state(sport_label, live_same_player_session_key, "live_same_player", False)
sync_typed_view_preference_state(sport_label, demo_legs_session_key, "demo_legs", 3, int)
sync_typed_view_preference_state(sport_label, demo_min_conf_session_key, "demo_min_confidence", 70, int)
sync_view_preference_state(sport_label, demo_style_session_key, "demo_parlay_style", "Safe")
sync_bool_view_preference_state(sport_label, demo_same_team_session_key, "demo_same_team", False)
smart_parlay_profiles = build_smart_parlay_profiles(get_ticket_summary_with_grades(sport_label))
render_shell_header(sport_label, sport_provider, board_type, sync_enabled, last_sync)
top_priority_board = get_latest_board(live_sport_keys, is_dfs=is_dfs) if live_sport_keys else pd.DataFrame()
top_priority_edges = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs) if live_sport_keys else pd.DataFrame()
top_priority_edges = apply_market_coverage(top_priority_edges, market_coverage_map) if not top_priority_edges.empty else top_priority_edges
top_priority_watchlist_alerts = get_watchlist_alerts(top_priority_edges, sport_label) if not top_priority_edges.empty else pd.DataFrame()
top_priority_unresolved = get_unresolved_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
top_priority_graded = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
top_priority_tickets = get_ticket_summary_with_grades(sport_label)
top_priority_journal = get_journal_entries(sport_label)
top_priority_weekly_review = build_weekly_model_review(top_priority_graded)
top_priority_monthly_review = build_monthly_model_review(top_priority_graded)
top_priority_source_summary = build_true_source_summary(top_priority_graded)
top_priority_auto_weight_profile = build_smart_weight_profile(top_priority_graded)
top_priority_resolved_weight_profile = apply_smart_weight_overrides(top_priority_auto_weight_profile, manual_smart_weight_overrides)
top_priority_operating_mode = build_overview_operating_mode(
    source_summary_df=top_priority_source_summary,
    weekly_review=top_priority_weekly_review,
    monthly_review=top_priority_monthly_review,
    sport_label=sport_label,
    is_dfs=is_dfs,
    overview_watchlist_alerts=top_priority_watchlist_alerts,
    overview_unresolved_tracked=top_priority_unresolved,
    overview_edges=top_priority_edges,
)
top_priority_cards = build_overview_next_step_cards(
    overview_board=top_priority_board,
    overview_edges=top_priority_edges,
    overview_watchlist_alerts=top_priority_watchlist_alerts,
    overview_tickets=top_priority_tickets,
    overview_unresolved_tracked=top_priority_unresolved,
    overview_graded=top_priority_graded,
    overview_journal=top_priority_journal,
    source_summary_df=top_priority_source_summary,
    weekly_review=top_priority_weekly_review,
    monthly_review=top_priority_monthly_review,
    sport_label=sport_label,
    is_dfs=is_dfs,
)
render_top_priority_strip(
    top_priority_operating_mode,
    top_priority_cards,
    source_summary_df=top_priority_source_summary,
    edges_df=top_priority_edges,
    watchlist_alerts_df=top_priority_watchlist_alerts,
    weekly_review=top_priority_weekly_review,
    monthly_review=top_priority_monthly_review,
    smart_weight_profile=top_priority_resolved_weight_profile,
    manual_override_enabled=bool(manual_smart_weight_overrides),
    last_sync=last_sync,
    sync_enabled=sync_enabled,
    collapsed=bool(st.session_state.get("top_priority_strip_collapsed", False)),
    sport_label=sport_label,
    is_dfs=is_dfs,
)
if not sync_enabled:
    st.info("This sport is routed through the esports provider slot. Demo/live-seeded views work now; external esports API integration is the next step.")

with st.expander("Market Coverage", expanded=False):
    if market_coverage_df.empty:
        st.caption("No market coverage metadata is configured yet.")
    else:
        st.dataframe(market_coverage_df, use_container_width=True, hide_index=True)

with st.expander("Watchlist", expanded=False):
    st.caption("Save props you want to revisit across the live board, edge scanner, and overview.")
    alert_col1, alert_col2 = st.columns(2)
    watchlist_min_edge = alert_col1.number_input(
        "Alert edge threshold (%)",
        min_value=0.0,
        max_value=50.0,
        value=float(watchlist_alert_settings["min_edge_pct"]),
        step=0.5,
        key="watchlist_min_edge_pct",
    )
    watchlist_min_confidence = alert_col2.number_input(
        "Alert confidence threshold",
        min_value=0.0,
        max_value=100.0,
        value=float(watchlist_alert_settings["min_confidence"]),
        step=1.0,
        key="watchlist_min_confidence",
    )
    if st.button("Save Watchlist Alert Settings", use_container_width=True):
        save_watchlist_alert_settings(
            sport_label,
            min_edge_pct=float(watchlist_min_edge),
            min_confidence=float(watchlist_min_confidence),
        )
        st.success("Saved watchlist alert thresholds.")
        st.rerun()
    if watchlist_df.empty:
        st.caption("No watchlist entries yet. Add rows from Live Board or Edge Scanner.")
    else:
        watchlist_display = watchlist_df[
            [col for col in ["player_display", "player_team", "market", "pick", "sportsbook", "line", "price", "created_at"] if col in watchlist_df.columns]
        ].copy()
        if "player_display" in watchlist_display.columns:
            watchlist_display = watchlist_display.rename(columns={"player_display": "player"})
        st.dataframe(
            compact_numeric_table(watchlist_display),
            use_container_width=True,
            hide_index=True,
        )
        watchlist_remove_options = {
            f"{row.get('player_display') or row['player']} | {row['market']} | {row.get('pick', '')}": row["watchlist_key"]
            for _, row in watchlist_df.iterrows()
        }
        selected_watchlist_removals = st.multiselect(
            "Remove watchlist entries",
            options=list(watchlist_remove_options.keys()),
            key="watchlist_remove_options",
        )
        if st.button("Remove Selected Watchlist Entries", use_container_width=True):
            removed = remove_watchlist_keys(
                sport_label,
                [watchlist_remove_options[label] for label in selected_watchlist_removals],
            )
            if removed > 0:
                st.success(f"Removed {removed} watchlist entries.")
            else:
                st.info("No watchlist entries were removed.")
            st.rerun()
        if st.button("Build Next Live Ticket From Watchlist Alerts", use_container_width=True, key="watchlist_promote_alerts"):
            promote_watchlist_alerts_to_parlay_lab()
            st.success("Parlay Lab is now set to build from watchlist alerts.")
            st.rerun()

with st.expander("View Preferences", expanded=False):
    st.caption("Remembered per sport after you change them. Reset here if you want to return to the default compact views.")
    view_pref_col1, view_pref_col2 = st.columns(2)
    view_pref_col1.write(f"Selected Sport: `{get_view_preference('__app__', 'selected_sport_label', sport_labels[0])}`")
    view_pref_col1.write(f"Theme: `{get_view_preference('__app__', 'theme_mode', 'Light')}`")
    view_pref_col1.write(f"Board Type: `{get_view_preference(sport_label, 'board_type', 'Sportsbook')}`")
    view_pref_col1.write(f"Live Board: `{get_view_preference(sport_label, 'board_view_mode', 'Compact')}`")
    view_pref_col1.write(f"Board Show Non-Live: `{get_view_preference(sport_label, 'show_non_live_board', str(not sync_enabled))}`")
    view_pref_col1.write(f"Board Market Filter: `{get_view_preference(sport_label, 'board_market_filter', '') or 'Any'}`")
    view_pref_col1.write(f"Board Sort By: `{get_view_preference(sport_label, 'board_sort_by', 'pulled_at')}`")
    view_pref_col1.write(f"Board Ascending: `{get_view_preference(sport_label, 'board_sort_ascending', 'False')}`")
    view_pref_col1.write(f"Board Watchlist Only: `{get_view_preference(sport_label, 'board_watchlist_only', 'False')}`")
    view_pref_col1.write(f"Edge Scanner: `{get_view_preference(sport_label, 'edge_view_mode', 'Compact')}`")
    view_pref_col1.write(f"Edge Show Non-Live: `{get_view_preference(sport_label, 'show_non_live_edges', 'False')}`")
    view_pref_col1.write(f"Edge Market Filter: `{get_view_preference(sport_label, 'edge_market_filter', '') or 'Any'}`")
    view_pref_col1.write(f"Edge Sort By: `{get_view_preference(sport_label, 'edge_sort_by', 'confidence')}`")
    view_pref_col1.write(f"Edge Ascending: `{get_view_preference(sport_label, 'edge_sort_ascending', 'False')}`")
    view_pref_col1.write(f"Edge Watchlist Only: `{get_view_preference(sport_label, 'edge_watchlist_only', 'False')}`")
    view_pref_col1.write(f"Edge Alerts Only: `{get_view_preference(sport_label, 'edge_alerts_only', 'False')}`")
    view_pref_col1.write(f"Live Legs: `{get_view_preference(sport_label, 'live_legs', '3')}`")
    view_pref_col1.write(f"Live Min Confidence: `{get_view_preference(sport_label, 'live_min_confidence', '65')}`")
    view_pref_col2.write(f"Parlay Source: `{get_view_preference(sport_label, 'parlay_source', 'Live edges')}`")
    view_pref_col2.write(f"Live Parlay Lab: `{get_view_preference(sport_label, 'parlay_view_mode', 'Compact')}`")
    view_pref_col2.write(f"Demo Parlay Lab: `{get_view_preference(sport_label, 'demo_parlay_view_mode', 'Compact')}`")
    view_pref_col2.write(f"History Player Suggestion: `{get_view_preference(sport_label, 'history_player_suggestion', '') or 'None'}`")
    view_pref_col2.write(f"History Market Suggestion: `{get_view_preference(sport_label, 'history_market_suggestion', '') or 'None'}`")
    view_pref_col2.write(f"Graded Market Filter: `{get_view_preference(sport_label, 'graded_market_filter', '') or 'Any'}`")
    view_pref_col2.write(f"Graded Sort By: `{get_view_preference(sport_label, 'graded_sort_by', 'resolved_at')}`")
    view_pref_col2.write(f"Demo Legs: `{get_view_preference(sport_label, 'demo_legs', '3')}`")
    view_pref_col2.write(f"Demo Min Confidence: `{get_view_preference(sport_label, 'demo_min_confidence', '70')}`")
    view_pref_col2.write(f"Demo Style: `{get_view_preference(sport_label, 'demo_parlay_style', 'Safe')}`")
    if st.button("Reset Views To Default", use_container_width=True, key="reset_view_preferences_button"):
        reset_view_preferences(sport_label)
        st.session_state[board_type_session_key] = "Sportsbook"
        st.session_state[parlay_source_session_key] = "Live edges"
        st.session_state[board_view_session_key] = "Compact"
        st.session_state[show_non_live_board_session_key] = not sync_enabled
        st.session_state[board_market_filter_session_key] = ""
        st.session_state[board_sort_by_session_key] = "pulled_at"
        st.session_state[board_sort_ascending_session_key] = False
        st.session_state[board_watchlist_only_session_key] = False
        st.session_state[edge_view_session_key] = "Compact"
        st.session_state[show_non_live_edges_session_key] = False
        st.session_state[history_player_suggestion_session_key] = ""
        st.session_state[history_market_suggestion_session_key] = ""
        st.session_state[graded_market_filter_session_key] = ""
        st.session_state[graded_sort_by_session_key] = "resolved_at"
        st.session_state[edge_market_filter_session_key] = ""
        st.session_state[edge_sort_by_session_key] = "confidence"
        st.session_state[edge_sort_ascending_session_key] = False
        st.session_state[edge_watchlist_only_session_key] = False
        st.session_state[edge_alerts_only_session_key] = False
        st.session_state[parlay_view_session_key] = "Compact"
        st.session_state[demo_parlay_view_session_key] = "Compact"
        st.session_state[live_legs_session_key] = 3
        st.session_state[live_min_conf_session_key] = 65
        st.session_state[demo_legs_session_key] = 3
        st.session_state[demo_min_conf_session_key] = 70
        st.session_state[demo_style_session_key] = "Safe"
        st.success("View preferences reset to Compact for this sport.")
        st.rerun()

with st.sidebar:
    st.subheader("Demo Live Data")
    st.caption("Populate the live tabs with local sample events, odds, projections, and line-history snapshots.")

    st.divider()
    st.subheader("SportsGameOdds Guard")
    if sgo_usage.get("auth_error"):
        st.info(sgo_usage.get("message"))
        if sgo_usage.get("detail"):
            st.caption(str(sgo_usage.get("detail")))
    elif sgo_usage.get("enabled"):
        if sgo_usage.get("ok_to_sync"):
            st.success(sgo_usage.get("message"))
        else:
            st.warning(sgo_usage.get("message"))
        if sgo_usage.get("detail"):
            st.caption(str(sgo_usage.get("detail")))

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

        if st.button(
            f"Sync Live {sport_label} Now",
            use_container_width=True,
            disabled=bool(sgo_usage.get("auth_error")),
        ):
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
                st.info("No live `player_first_basket` rows are currently in the board.")

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
    overview_board = annotate_watchlist_movement(overview_board, sport_label) if not overview_board.empty else overview_board
    overview_board = annotate_player_display(overview_board) if not overview_board.empty else overview_board
    overview_edges = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs) if live_sport_keys else pd.DataFrame()
    overview_edges = apply_market_coverage(overview_edges, market_coverage_map) if not overview_edges.empty else overview_edges
    overview_edges = annotate_stake_recommendations(
        overview_edges,
        bankroll=bankroll_amount,
        unit_size=unit_size,
        kelly_fraction_cap=fractional_kelly,
        max_units=max_bet_units,
    ) if not overview_edges.empty else overview_edges
    overview_edges = annotate_watchlist_movement(overview_edges, sport_label) if not overview_edges.empty else overview_edges
    overview_edges = annotate_player_display(overview_edges) if not overview_edges.empty else overview_edges
    overview_watchlist_alerts = get_watchlist_alerts(overview_edges, sport_label) if not overview_edges.empty else pd.DataFrame()
    overview_tracked = get_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    overview_unresolved_tracked = get_unresolved_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    overview_graded = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    overview_smart_edges, overview_smart_summary = score_smart_picks(overview_edges, overview_graded, override_profile=manual_smart_weight_overrides)
    overview_weekly_review = build_weekly_model_review(overview_graded)
    overview_monthly_review = build_monthly_model_review(overview_graded)
    overview_source_summary = build_true_source_summary(overview_graded)
    overview_review_action_checklist = build_review_action_checklist(overview_weekly_review, overview_monthly_review)
    overview_journal = get_journal_entries(sport_label)
    overview_bankroll = build_bankroll_summary(overview_journal, bankroll_amount)
    overview_kpis = build_bankroll_kpis(overview_journal, bankroll_amount)
    overview_watchlist = get_watchlist_df(sport_label)
    overview_watchlist = annotate_player_display(overview_watchlist) if not overview_watchlist.empty else overview_watchlist
    overview_tickets = get_ticket_summary_with_grades(sport_label)
    overview_notifications = build_notification_center(
        watchlist_alerts=overview_watchlist_alerts,
        unresolved_tracked=overview_unresolved_tracked,
        saved_tickets=overview_tickets,
        journal_df=overview_journal,
    )
    visible_notifications = [
        notice for notice in overview_notifications if is_notification_visible(sport_label, str(notice.get("notice_id", "")))
    ]
    hidden_notification_history = get_notification_history_rows(sport_label)

    st.markdown("### Coach Mode")
    st.info(build_coach_mode_summary(overview_weekly_review, overview_monthly_review, sport_label=sport_label))
    overview_operating_mode = build_overview_operating_mode(
        source_summary_df=overview_source_summary,
        weekly_review=overview_weekly_review,
        monthly_review=overview_monthly_review,
        sport_label=sport_label,
        is_dfs=is_dfs,
        overview_watchlist_alerts=overview_watchlist_alerts,
        overview_unresolved_tracked=overview_unresolved_tracked,
        overview_edges=overview_edges,
    )
    st.markdown("### Today's Operating Mode")
    st.markdown(
        f"""
        <div style="
            background: {theme['card_bg']};
            border: 1px solid {theme['card_border']};
            border-radius: 20px;
            padding: 1rem 1.1rem;
            margin: 0 0 0.8rem;
            box-shadow: 0 10px 24px rgba(8, 15, 28, 0.08);
        ">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:1rem;flex-wrap:wrap;">
                <div style="font-size:1.02rem;font-weight:700;color:{theme['heading_text']};">{overview_operating_mode['title']}</div>
                <div style="
                    padding:0.22rem 0.7rem;
                    border-radius:999px;
                    border:1px solid {theme['card_border']};
                    color:{theme['section_subtitle']};
                    font-size:0.78rem;
                    font-weight:700;
                    letter-spacing:0.03em;
                    text-transform:uppercase;
                ">{overview_operating_mode['confidence']}</div>
            </div>
            <div style="margin-top:0.55rem;color:{theme['body_text']};line-height:1.5;">
                {overview_operating_mode['body']}
            </div>
            <div style="margin-top:0.6rem;color:{theme['section_subtitle']};font-weight:700;">
                Suggested default workflow: {overview_operating_mode['default_workflow']}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    operating_mode_target_map = {
        "Edge Scanner": "edge_scanner",
        "Parlay Lab": "parlay_lab",
        "Results & Grading": "results_grading",
        "Backtest": "backtest",
        "Live Board": "live_board",
    }
    operating_mode_target = operating_mode_target_map.get(overview_operating_mode["default_workflow"], "overview")
    operating_mode_col1, operating_mode_col2 = st.columns(2)
    if operating_mode_col1.button(
        f"Focus {overview_operating_mode['default_workflow']}",
        key="overview_open_default_workflow",
        use_container_width=True,
    ):
        set_dashboard_focus(operating_mode_target)
        if operating_mode_target == "results_grading":
            set_results_grading_focus("saved_tickets")
        st.rerun()
    if operating_mode_col2.button(
        "Apply Current Review Checklist",
        key="overview_apply_review_checklist",
        use_container_width=True,
    ):
        apply_review_action_checklist(
            sport_label=sport_label,
            checklist=overview_review_action_checklist,
            live_legs_session_key=live_legs_session_key,
            live_min_conf_session_key=live_min_conf_session_key,
            live_same_player_session_key=live_same_player_session_key,
            demo_style_session_key=demo_style_session_key,
            demo_same_team_session_key=demo_same_team_session_key,
        )
        set_dashboard_focus("parlay_lab")
        st.success("Applied the current review checklist and moved the app toward Parlay Lab.")
        st.rerun()
    overview_next_step_cards = build_overview_next_step_cards(
        overview_board=overview_board,
        overview_edges=overview_edges,
        overview_watchlist_alerts=overview_watchlist_alerts,
        overview_tickets=overview_tickets,
        overview_unresolved_tracked=overview_unresolved_tracked,
        overview_graded=overview_graded,
        overview_journal=overview_journal,
        source_summary_df=overview_source_summary,
        weekly_review=overview_weekly_review,
        monthly_review=overview_monthly_review,
        sport_label=sport_label,
        is_dfs=is_dfs,
    )
    render_recommendation_cards(overview_next_step_cards, "Suggested Next Steps", key_prefix="overview_next_steps")

    if not overview_watchlist_alerts.empty:
        st.markdown("### Top Watchlist Signals")
        alert_cards = overview_watchlist_alerts.head(3)
        alert_card_cols = st.columns(len(alert_cards))
        for idx, (_, alert_row) in enumerate(alert_cards.iterrows()):
            with alert_card_cols[idx]:
                render_watchlist_alert_card_with_reason(
                    alert_row,
                    threshold_edge_pct=float(watchlist_alert_settings["min_edge_pct"]),
                    threshold_confidence=float(watchlist_alert_settings["min_confidence"]),
                )
        if st.button("Promote Watchlist Alerts To Parlay Lab", use_container_width=True, key="overview_promote_watchlist_alerts"):
            promote_watchlist_alerts_to_parlay_lab()
            st.success("Parlay Lab is now set to build from watchlist alerts.")
            st.rerun()

    st.markdown("### Notification Center")
    if not visible_notifications:
        render_empty_state("No urgent dashboard items", "Your watchlist, tickets, tracked picks, and bankroll reminders are all quiet right now.", tone="info")
    else:
        reset_notice_options = {
            notice["title"]: notice["notice_id"]
            for notice in overview_notifications
            if notice.get("notice_id")
        }
        selected_reset_notice = st.selectbox(
            "Restore hidden reminder",
            options=[""] + list(reset_notice_options.keys()),
            key="notification_reset_select",
        )
        if st.button("Restore Reminder", key="restore_notification_button", use_container_width=False):
            if selected_reset_notice:
                reset_notification(sport_label, str(reset_notice_options[selected_reset_notice]))
                st.success("Restored the selected reminder.")
                st.rerun()
            else:
                st.info("Choose a reminder to restore first.")
        for idx, notice in enumerate(visible_notifications):
            render_notification_notice(notice, key_suffix=str(idx), sport_label=sport_label)
    if hidden_notification_history:
        with st.expander("Notification History", expanded=False):
            history_df = pd.DataFrame(hidden_notification_history)
            if "timestamp" in history_df.columns:
                history_df["when"] = history_df["timestamp"].map(lambda value: format_relative_timestamp(value))
            st.dataframe(history_df, use_container_width=True, hide_index=True)

    st.markdown("### Workflow Readiness")
    workflow_left, workflow_right = st.columns(2)
    with workflow_left:
        render_workflow_check_item(
            "Market data available",
            ok=not overview_board.empty,
            detail=(
                f"{len(overview_board)} live board rows are loaded."
                if not overview_board.empty
                else "Run a live sync or seed demo data so the board can populate."
            ),
        )
        render_workflow_check_item(
            "Edge scanner populated",
            ok=not overview_edges.empty,
            detail=(
                f"{len(overview_edges)} edge rows are available for ranking."
                if not overview_edges.empty
                else "Wait for market lines and projections so the scanner can rank props."
            ),
        )
        render_workflow_check_item(
            "Watchlist active",
            ok=not overview_watchlist.empty,
            detail=(
                f"{len(overview_watchlist)} props are saved to the watchlist."
                if not overview_watchlist.empty
                else "Add a few props from Live Board or Edge Scanner to test the watchlist workflow."
            ),
        )
    with workflow_right:
        render_workflow_check_item(
            "Ticket tracking started",
            ok=not overview_tickets.empty,
            detail=(
                f"{len(overview_tickets)} saved tickets are available."
                if not overview_tickets.empty
                else "Save a live or demo ticket from Parlay Lab to test ticket tracking."
            ),
        )
        render_workflow_check_item(
            "Grading history exists",
            ok=not overview_graded.empty,
            detail=(
                f"{len(overview_graded)} graded picks are ready for backtest views."
                if not overview_graded.empty
                else "Track edges and settle results to unlock true-results backtesting."
            ),
        )
        render_workflow_check_item(
            "Bankroll journal active",
            ok=not overview_journal.empty,
            detail=(
                f"{len(overview_journal)} bankroll entries are logged."
                if not overview_journal.empty
                else "Add a manual journal entry or log a saved ticket to test bankroll tracking."
            ),
        )

    overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
    overview_col1.metric("Live Board Rows", f"{len(overview_board)}")
    overview_col2.metric("Live Edge Rows", f"{len(overview_edges)}")
    overview_col3.metric("Tracked Picks", f"{len(overview_tracked)}")
    overview_col4.metric("Watchlist Alerts", f"{len(overview_watchlist_alerts)}")

    overview_col5, overview_col6, overview_col7, overview_col8 = st.columns(4)
    overview_col5.metric("Current Bankroll", f"${overview_bankroll['current_bankroll']}")
    overview_col6.metric("Open Risk", f"${overview_bankroll['open_risk']}")
    overview_col7.metric("ROI", f"{overview_kpis['roi'] * 100:.2f}%")
    overview_col8.metric("Graded Picks", f"{len(overview_graded)}")

    render_smart_pick_section(
        scored_df=overview_smart_edges,
        history_summary=overview_smart_summary,
        title="Smart Pick Engine",
        body="This ranking blends live model edge with your graded history by market, sportsbook, and confidence band so the app can start learning what has actually worked.",
        top_n=6,
    )

    left_col, right_col = st.columns(2)
    with left_col:
        st.markdown("### Watchlist Alerts")
        if overview_watchlist_alerts.empty:
            render_empty_state("No active watchlist alerts", "None of your watched props currently meet the saved alert thresholds.", tone="neutral")
        else:
            display_watchlist = overview_watchlist_alerts.head(10)[
                [col for col in ["player_display", "player_team", "market", "pick", "sportsbook", "edge", "confidence", "line_move", "line_move_label", "price_move", "price_move_label", "recommended_units", "recommended_stake"] if col in overview_watchlist_alerts.columns]
            ].copy()
            if "player_display" in display_watchlist.columns:
                display_watchlist = display_watchlist.rename(columns={"player_display": "player"})
            if "market" in display_watchlist.columns:
                display_watchlist["market"] = display_watchlist["market"].map(prettify_market_label)
            if "edge" in display_watchlist.columns:
                display_watchlist["edge"] = (display_watchlist["edge"] * 100).round(2)
            display_watchlist = prettify_table_headers(display_watchlist)
            st.dataframe(style_signal_table(compact_numeric_table(display_watchlist)), use_container_width=True, hide_index=True)

    with right_col:
        st.markdown("### Top Live Edges")
        if overview_edges.empty:
            render_empty_state("No live edges yet", "Run a sync or seed demo data to populate the current live edge board.", tone="info")
        else:
            top_overview_edges = overview_edges[overview_edges["coverage_status"] == "Live"].copy() if "coverage_status" in overview_edges.columns else overview_edges.copy()
            top_overview_edges = top_overview_edges.sort_values(["confidence", "edge"], ascending=False).head(10)
            if not top_overview_edges.empty:
                display_overview_edges = top_overview_edges[
                    [col for col in ["player_display", "player_team", "market", "pick", "sportsbook", "edge", "confidence", "recommended_units", "recommended_stake"] if col in top_overview_edges.columns]
                ].copy()
                if "player_display" in display_overview_edges.columns:
                    display_overview_edges = display_overview_edges.rename(columns={"player_display": "player"})
                if "market" in display_overview_edges.columns:
                    display_overview_edges["market"] = display_overview_edges["market"].map(prettify_market_label)
                display_overview_edges["edge"] = (display_overview_edges["edge"] * 100).round(2)
                display_overview_edges = prettify_table_headers(display_overview_edges)
                st.dataframe(style_signal_table(compact_numeric_table(display_overview_edges)), use_container_width=True, hide_index=True)

    overview_ticket_col, overview_saved_col = st.columns(2)
    with overview_ticket_col:
        st.markdown("### Saved Ticket Snapshot")
        if overview_tickets.empty:
            render_empty_state("No saved tickets yet", "Build a ticket in Parlay Lab to start tracking live or demo slips here.", tone="neutral")
        else:
            st.dataframe(
                overview_tickets[
                    ["ticket_id", "name", "source", "leg_count", "avg_confidence", "ticket_status_live", "created_at"]
                ].head(10),
                use_container_width=True,
                hide_index=True,
            )
    with overview_saved_col:
        st.markdown("### Watchlist Snapshot")
        watchlist_snapshot = overview_edges[overview_edges["is_watchlisted"]].copy() if not overview_edges.empty and "is_watchlisted" in overview_edges.columns else pd.DataFrame()
        if watchlist_snapshot.empty:
            render_empty_state("No active watchlist props", "Add rows from Live Board or Edge Scanner to start monitoring prop movement.", tone="neutral")
        else:
            watchlist_snapshot = watchlist_snapshot.sort_values(["confidence", "edge"], ascending=False).head(10)
            display_watchlist_snapshot = watchlist_snapshot[
                [col for col in ["player_display", "player_team", "market", "pick", "sportsbook", "edge", "confidence", "line_move", "line_move_label", "price_move", "price_move_label"] if col in watchlist_snapshot.columns]
            ].copy()
            if "player_display" in display_watchlist_snapshot.columns:
                display_watchlist_snapshot = display_watchlist_snapshot.rename(columns={"player_display": "player"})
            if "market" in display_watchlist_snapshot.columns:
                display_watchlist_snapshot["market"] = display_watchlist_snapshot["market"].map(prettify_market_label)
            if "edge" in display_watchlist_snapshot.columns:
                display_watchlist_snapshot["edge"] = (display_watchlist_snapshot["edge"] * 100).round(2)
            display_watchlist_snapshot = prettify_table_headers(display_watchlist_snapshot)
            st.dataframe(style_signal_table(compact_numeric_table(display_watchlist_snapshot)), use_container_width=True, hide_index=True)

with tab1:
    render_section_header("Live Board", "Inspect the latest normalized market rows with quick filters and export controls.")
    board = pd.DataFrame()

    if live_sport_keys:
        board = get_latest_board(live_sport_keys, is_dfs=is_dfs)

    if board.empty:
        render_empty_state("No live board data", "Run a sync or seed demo data before loading the live board.", tone="warning")
    else:
        board = apply_market_coverage(board, market_coverage_map)
        board = annotate_watchlist_movement(board, sport_label)
        board = annotate_player_display(board)
        board_view_mode = st.radio(
            "Board view",
            ["Compact", "Expanded"],
            horizontal=True,
            key=board_view_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, board_view_session_key, "board_view_mode"),
        )
        show_non_live_board = st.checkbox(
            "Show demo-only/provider-unavailable markets",
            key=show_non_live_board_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, show_non_live_board_session_key, "show_non_live_board"),
        )
        persist_preference_if_changed(sport_label, "show_non_live_board", show_non_live_board, not sync_enabled)
        display_board = board if show_non_live_board else board[board["coverage_status"] == "Live"].copy()
        board_filter_col1, board_filter_col2, board_filter_col3, board_filter_col4, board_filter_col5 = st.columns(5)
        board_market_filter = board_filter_col1.selectbox(
            "Market filter",
            [""] + sorted(display_board["market"].dropna().astype(str).unique().tolist()) if not display_board.empty else [""],
            key=board_market_filter_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, board_market_filter_session_key, "board_market_filter"),
        )
        board_player_filter = board_filter_col2.text_input("Player search", key="board_player_filter")
        board_sort_by = board_filter_col3.selectbox(
            "Sort by",
            [col for col in ["pulled_at", "line", "price", "player", "market"] if col in display_board.columns] if not display_board.empty else [""],
            key=board_sort_by_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, board_sort_by_session_key, "board_sort_by"),
        )
        board_sort_ascending = board_filter_col4.checkbox(
            "Ascending",
            key=board_sort_ascending_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, board_sort_ascending_session_key, "board_sort_ascending"),
        )
        board_watchlist_only = board_filter_col5.checkbox(
            "Watchlist only",
            key=board_watchlist_only_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, board_watchlist_only_session_key, "board_watchlist_only"),
        )
        persist_preference_if_changed(sport_label, "board_market_filter", board_market_filter, "")
        persist_preference_if_changed(sport_label, "board_sort_by", board_sort_by, "pulled_at")
        persist_preference_if_changed(sport_label, "board_sort_ascending", board_sort_ascending, False)
        persist_preference_if_changed(sport_label, "board_watchlist_only", board_watchlist_only, False)
        display_board = filter_dataframe(
            display_board,
            market_key=board_market_filter,
            player_query=board_player_filter,
            sort_by=board_sort_by,
            ascending=board_sort_ascending,
        )
        if board_watchlist_only and "is_watchlisted" in display_board.columns:
            display_board = display_board[display_board["is_watchlisted"]].copy()
        if display_board.empty:
            render_empty_state("No rows match this board view", "Try relaxing the filters or showing demo-only/provider-unavailable markets.", tone="info")
        else:
            display_board["watchlist"] = display_board["is_watchlisted"].map(lambda value: "Yes" if value else "")
            board_display = (
                build_clean_live_board_display(display_board)
                if board_view_mode == "Compact"
                else build_expanded_live_board_display(display_board)
            )
            st.dataframe(
                style_signal_table(compact_numeric_table(board_display)),
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Export Live Board CSV",
                data=display_board.to_csv(index=False),
                file_name=f"{sport_label.lower()}_live_board.csv",
                mime="text/csv",
                use_container_width=True,
            )
            board_watchlist_options = build_watchlist_option_labels(display_board.head(30))
            selected_board_watchlist = st.multiselect(
                "Add board rows to watchlist",
                options=list(board_watchlist_options.keys()),
                key="board_watchlist_selection",
            )
            if st.button("Save Selected Board Rows To Watchlist", use_container_width=True):
                added = add_watchlist_rows(
                    display_board,
                    [board_watchlist_options[label] for label in selected_board_watchlist],
                    sport_label,
                )
                if added > 0:
                    st.success(f"Added {added} board rows to the watchlist.")
                else:
                    st.info("No new board rows were added to the watchlist.")
                st.rerun()

with tab2:
    render_section_header("Edge Scanner", "Rank live-supported props by model edge, confidence, and suggested stake size.")
    if st.session_state.get("dashboard_focus_target") == "edge_scanner":
        st.success("Workflow focus is set to Edge Scanner. This is the fastest place to save live edges for grading or watchlist review.")
    edge_df = pd.DataFrame()

    if live_sport_keys:
        edge_df = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs)

    if edge_df.empty:
        render_empty_state("No edge data found", "You may need synced market lines and saved projections before the scanner can rank props.", tone="warning")
    else:
        edge_df = apply_market_coverage(edge_df, market_coverage_map)
        edge_df = annotate_stake_recommendations(
            edge_df,
            bankroll=bankroll_amount,
            unit_size=unit_size,
            kelly_fraction_cap=fractional_kelly,
            max_units=max_bet_units,
        )
        edge_df = annotate_watchlist_movement(edge_df, sport_label)
        edge_df = annotate_player_display(edge_df)
        edge_graded_history = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
        edge_df, edge_smart_summary = score_smart_picks(edge_df, edge_graded_history, override_profile=manual_smart_weight_overrides)
        edge_view_mode = st.radio(
            "Edge view",
            ["Compact", "Expanded"],
            horizontal=True,
            key=edge_view_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, edge_view_session_key, "edge_view_mode"),
        )
        alert_watchlist_edges = get_watchlist_alerts(edge_df, sport_label)
        show_non_live_edges = st.checkbox(
            "Show demo-only/provider-unavailable edge rows",
            key=show_non_live_edges_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, show_non_live_edges_session_key, "show_non_live_edges"),
        )
        persist_preference_if_changed(sport_label, "show_non_live_edges", show_non_live_edges, False)
        display_edges = edge_df if show_non_live_edges else edge_df[edge_df["coverage_status"] == "Live"].copy()
        edge_filter_col1, edge_filter_col2, edge_filter_col3, edge_filter_col4, edge_filter_col5, edge_filter_col6 = st.columns(6)
        edge_market_filter = edge_filter_col1.selectbox(
            "Market filter",
            [""] + sorted(display_edges["market"].dropna().astype(str).unique().tolist()) if not display_edges.empty else [""],
            key=edge_market_filter_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, edge_market_filter_session_key, "edge_market_filter"),
        )
        edge_player_filter = edge_filter_col2.text_input("Player search", key="edge_player_filter")
        edge_sort_by = edge_filter_col3.selectbox(
            "Sort by",
            [col for col in ["smart_score", "confidence", "edge", "model_prob", "recommended_stake", "player"] if col in display_edges.columns] if not display_edges.empty else [""],
            key=edge_sort_by_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, edge_sort_by_session_key, "edge_sort_by"),
        )
        edge_sort_ascending = edge_filter_col4.checkbox(
            "Ascending",
            key=edge_sort_ascending_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, edge_sort_ascending_session_key, "edge_sort_ascending"),
        )
        edge_watchlist_only = edge_filter_col5.checkbox(
            "Watchlist only",
            key=edge_watchlist_only_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, edge_watchlist_only_session_key, "edge_watchlist_only"),
        )
        edge_alerts_only = edge_filter_col6.checkbox(
            "Alerts only",
            key=edge_alerts_only_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, edge_alerts_only_session_key, "edge_alerts_only"),
        )
        persist_preference_if_changed(sport_label, "edge_market_filter", edge_market_filter, "")
        persist_preference_if_changed(sport_label, "edge_sort_by", edge_sort_by, "confidence")
        persist_preference_if_changed(sport_label, "edge_sort_ascending", edge_sort_ascending, False)
        persist_preference_if_changed(sport_label, "edge_watchlist_only", edge_watchlist_only, False)
        persist_preference_if_changed(sport_label, "edge_alerts_only", edge_alerts_only, False)
        display_edges = filter_dataframe(
            display_edges,
            market_key=edge_market_filter,
            player_query=edge_player_filter,
            sort_by=edge_sort_by,
            ascending=edge_sort_ascending,
        )
        if edge_watchlist_only and "is_watchlisted" in display_edges.columns:
            display_edges = display_edges[display_edges["is_watchlisted"]].copy()
        if edge_alerts_only:
            alert_keys = set(alert_watchlist_edges.get("watchlist_key", pd.Series(dtype=str)).tolist())
            if "watchlist_key" in display_edges.columns:
                display_edges = display_edges[display_edges["watchlist_key"].isin(alert_keys)].copy()
        if display_edges.empty:
            render_empty_state("No rows match this edge view", "Try relaxing the filters, thresholds, or watchlist-only alert view.", tone="info")
        else:
            display_edges["watchlist"] = display_edges["is_watchlisted"].map(lambda value: "Yes" if value else "")
            render_smart_pick_section(
                scored_df=display_edges,
                history_summary=edge_smart_summary,
                title="Smart-Ranked Edges",
                body="These candidates are ordered with the same smart-pick engine so you can compare live signals against your actual graded history before saving or tracking them.",
                top_n=8,
            )
            edge_display = (
                build_clean_edge_display(display_edges)
                if edge_view_mode == "Compact"
                else build_expanded_edge_display(display_edges)
            )
            st.dataframe(style_signal_table(compact_numeric_table(edge_display)), use_container_width=True)
            st.download_button(
                "Export Edge Scanner CSV",
                data=display_edges.to_csv(index=False),
                file_name=f"{sport_label.lower()}_edge_scanner.csv",
                mime="text/csv",
                use_container_width=True,
            )
        st.caption(
            f"Watchlist alerts use edge >= {watchlist_alert_settings['min_edge_pct']:.1f}% "
            f"and confidence >= {watchlist_alert_settings['min_confidence']:.1f}."
        )

        st.markdown("### Best Prop Cards")
        cards = build_prop_cards(display_edges.sort_values(["smart_score", "confidence", "edge"], ascending=False), top_n=10)

        for card in cards:
            render_prop_card(card)

        st.markdown("### Smart Score Audit")
        audit_candidates = display_edges.sort_values(["smart_score", "smart_expected_win_rate", "edge"], ascending=False).head(20).copy()
        if audit_candidates.empty:
            st.caption("No smart-ranked candidates are available to audit right now.")
        else:
            audit_options = audit_candidates["smart_audit_label"].tolist() if "smart_audit_label" in audit_candidates.columns else []
            selected_audit_pick = st.selectbox(
                "Inspect a smart-ranked pick",
                audit_options,
                key="smart_score_audit_pick",
            )
            selected_audit_row = audit_candidates[audit_candidates["smart_audit_label"] == selected_audit_pick].head(1)
            if not selected_audit_row.empty:
                audit_row = selected_audit_row.iloc[0]
                audit_metric_col1, audit_metric_col2, audit_metric_col3, audit_metric_col4 = st.columns(4)
                audit_metric_col1.metric("Smart score", f"{float(audit_row.get('smart_score', 0.0) or 0.0):.1f}")
                audit_metric_col2.metric("Expected win %", f"{float(audit_row.get('smart_expected_win_rate', 0.0) or 0.0) * 100:.1f}%")
                audit_metric_col3.metric("Profile mode", str(audit_row.get("smart_profile_mode") or "default").replace("_", " ").title())
                audit_metric_col4.metric("History picks used", f"{int(audit_row.get('history_picks_used', 0) or 0)}")
                st.caption(str(audit_row.get("smart_summary") or ""))

                history_compare_df = build_smart_history_comparison(audit_row)
                if not history_compare_df.empty:
                    st.markdown("#### Full History vs Recent Form")
                    history_compare_display = history_compare_df.copy()
                    history_compare_display["full_history"] = (pd.to_numeric(history_compare_display["full_history"], errors="coerce") * 100).round(1)
                    history_compare_display["recent_form"] = (pd.to_numeric(history_compare_display["recent_form"], errors="coerce") * 100).round(1)
                    history_compare_display["full_roi"] = pd.to_numeric(history_compare_display["full_roi"], errors="coerce").round(2)
                    history_compare_display["recent_roi"] = pd.to_numeric(history_compare_display["recent_roi"], errors="coerce").round(2)
                    history_compare_display = history_compare_display.rename(
                        columns={
                            "memory_type": "Memory Type",
                            "full_history": "Full Hit Rate %",
                            "recent_form": "Recent Hit Rate %",
                            "full_sample": "Full Picks",
                            "recent_sample": "Recent Picks",
                            "full_roi": "Full Units/Pick",
                            "recent_roi": "Recent Units/Pick",
                        }
                    )
                    st.dataframe(compact_numeric_table(history_compare_display), use_container_width=True, hide_index=True)

                audit_df = build_smart_pick_audit(audit_row)
                if not audit_df.empty:
                    st.dataframe(
                        compact_numeric_table(
                            audit_df.rename(
                                columns={
                                    "component": "Score Component",
                                    "impact": "Impact",
                                    "detail": "Why",
                                }
                            )
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )

        track_count = st.slider("Track top live edges", min_value=1, max_value=25, value=5, key="track_top_edges")
        if st.button("Save Top Live Edges For Grading", use_container_width=True):
            rows_to_track = display_edges.head(track_count).copy()
            tracked = track_edge_rows(rows_to_track, sport_key=live_sport_keys[0], source="edge_scanner")
            st.success(f"Saved {tracked} live edge rows to the grading tracker.")
        smart_track_count = st.slider("Track top smart-ranked edges", min_value=1, max_value=25, value=5, key="track_top_smart_edges")
        if st.button("Save Top Smart Picks For Grading", use_container_width=True):
            smart_rows_to_track = display_edges.sort_values(["smart_score", "smart_expected_win_rate", "edge"], ascending=False).head(smart_track_count).copy()
            smart_source = active_smart_tracking_source()
            tracked = track_edge_rows(smart_rows_to_track, sport_key=live_sport_keys[0], source=smart_source)
            st.success(f"Saved {tracked} smart-ranked picks to the grading tracker under `{format_source_label(smart_source)}`.")
        edge_watchlist_options = build_watchlist_option_labels(display_edges.head(30))
        selected_edge_watchlist = st.multiselect(
            "Add edge rows to watchlist",
            options=list(edge_watchlist_options.keys()),
            key="edge_watchlist_selection",
        )
        if st.button("Save Selected Edge Rows To Watchlist", use_container_width=True):
            added = add_watchlist_rows(
                display_edges,
                [edge_watchlist_options[label] for label in selected_edge_watchlist],
                sport_label,
            )
            if added > 0:
                st.success(f"Added {added} edge rows to the watchlist.")
            else:
                st.info("No new edge rows were added to the watchlist.")
            st.rerun()

with tab3:
    render_section_header("Parlay Lab", "Build live or demo tickets with clearer stake planning and model context.")
    if st.session_state.get("dashboard_focus_target") == "parlay_lab":
        st.success("Notification focus is set to Parlay Lab. This is the right place to turn strong watchlist alerts into a draft ticket.")
    parlay_focus_target = st.session_state.get("parlay_lab_section_focus_target", "")
    saved_ticket_override_active = st.session_state.get("parlay_source_session_override") == "saved_ticket"
    saved_ticket_payload = st.session_state.get("parlay_saved_ticket_payload", [])
    saved_ticket_source = str(st.session_state.get("parlay_saved_ticket_source", "live_edges"))
    saved_ticket_source_label = "Live edges" if saved_ticket_source == "live_edges" else "Demo predictions"
    if saved_ticket_override_active:
        saved_ticket_name = st.session_state.get("parlay_saved_ticket_name", "Saved ticket")
        saved_ticket_target = st.session_state.get("parlay_saved_ticket_target", "")
        target_suffix = f" for `{saved_ticket_target}`" if saved_ticket_target else ""
        st.info(
            f"Resumed from saved ticket `{saved_ticket_name}`{target_suffix}. "
            f"{len(saved_ticket_payload)} saved legs are available for rebuild or comparison in this session."
        )
        st.session_state[parlay_source_session_key] = saved_ticket_source_label
        if st.button("Clear Saved Ticket Context", key="clear_saved_ticket_context", use_container_width=False):
            for session_key in [
                "parlay_source_session_override",
                "parlay_saved_ticket_id",
                "parlay_saved_ticket_name",
                "parlay_saved_ticket_target",
                "parlay_saved_ticket_source",
                "parlay_saved_ticket_payload",
            ]:
                st.session_state.pop(session_key, None)
            st.rerun()
    source = st.radio(
        "Parlay Source",
        ["Live edges", "Demo predictions"],
        horizontal=True,
        key=parlay_source_session_key,
        on_change=persist_view_preference_from_session,
        args=(sport_label, parlay_source_session_key, "parlay_source"),
    )
    persist_preference_if_changed(sport_label, "parlay_source", source, "Live edges")
    parlay_jump_labels = [
        ("smart_profiles", "Jump to Smart Profiles"),
        ("builder", "Jump to Builder"),
        ("audit", "Jump to Leg Audit"),
    ]
    if is_dfs:
        parlay_jump_labels.append(("dfs_autoslip", "Jump to DFS Auto-Slip"))
    parlay_jump_cols = st.columns(len(parlay_jump_labels))
    for idx, (target, label) in enumerate(parlay_jump_labels):
        if parlay_jump_cols[idx].button(label, key=f"parlay_lab_jump_{target}", use_container_width=True):
            set_parlay_lab_focus(target)
            st.rerun()

    if source == "Live edges":
        selected_live_dfs_adapter = None
        edge_df = pd.DataFrame()

        if live_sport_keys:
            edge_df = scan_edges(sport_key=live_sport_keys, is_dfs=is_dfs)

        if edge_df.empty:
            render_empty_state("No live parlay candidates yet", "The live edge pool is empty, so Parlay Lab cannot rank legs right now.", tone="info")
        else:
            edge_df = apply_market_coverage(edge_df, market_coverage_map)
            edge_df = annotate_stake_recommendations(
                edge_df,
                bankroll=bankroll_amount,
                unit_size=unit_size,
                kelly_fraction_cap=fractional_kelly,
                max_units=max_bet_units,
            )
            edge_df = annotate_watchlist_movement(edge_df, sport_label)
            edge_df = annotate_player_display(edge_df)
            parlay_graded_history = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
            edge_df, _ = score_smart_picks(edge_df, parlay_graded_history, override_profile=manual_smart_weight_overrides)
            parlay_view_mode = st.radio(
                "Parlay table view",
                ["Compact", "Expanded"],
                horizontal=True,
                key=parlay_view_session_key,
                on_change=persist_view_preference_from_session,
                args=(sport_label, parlay_view_session_key, "parlay_view_mode"),
            )
            parlay_candidate_pool = st.radio(
                "Live candidate pool",
                ["All live edges", "Watchlist alerts"],
                horizontal=True,
                key="parlay_live_candidate_pool",
            )
            if st.session_state.get("parlay_live_use_watchlist_alerts"):
                st.info("Parlay Lab is currently focused on promoted watchlist alerts.")
            legs = st.slider(
                "Legs",
                min_value=2,
                max_value=6,
                key=live_legs_session_key,
                on_change=persist_view_preference_from_session,
                args=(sport_label, live_legs_session_key, "live_legs"),
            )
            min_confidence = st.slider(
                "Minimum confidence",
                min_value=50,
                max_value=95,
                key=live_min_conf_session_key,
                on_change=persist_view_preference_from_session,
                args=(sport_label, live_min_conf_session_key, "live_min_confidence"),
            )
            persist_preference_if_changed(sport_label, "live_legs", legs, 3)
            persist_preference_if_changed(sport_label, "live_min_confidence", min_confidence, 65)
            allow_same_player = st.checkbox(
                "Allow multiple picks on the same player",
                key=live_same_player_session_key,
                on_change=persist_view_preference_from_session,
                args=(sport_label, live_same_player_session_key, "live_same_player"),
            )
            persist_preference_if_changed(sport_label, "live_same_player", allow_same_player, False)
            if parlay_focus_target == "smart_profiles":
                st.info("Parlay Lab jump is focused on Smart Profiles below.")
            render_smart_parlay_profile_panel(
                profile=smart_parlay_profiles["live"],
                title="Smart Live Parlay Profile",
                mode_label="Live",
            current_values={
                "legs": legs,
                "min_confidence": min_confidence,
                "allow_overlap": allow_same_player,
            },
            apply_button_key=f"apply_live_smart_profile_{sport_label}",
            apply_callback=lambda profile: {
                live_legs_session_key: int(profile.get("recommended_legs", legs)),
                live_min_conf_session_key: int(profile.get("recommended_min_confidence", min_confidence)),
                live_same_player_session_key: bool(profile.get("recommended_same_player", allow_same_player)),
            },
            pending_updates_key=live_profile_pending_updates_key,
        )
            if is_dfs and str(smart_parlay_profiles["dfs"].get("recommended_target_label") or "").strip():
                st.caption(
                    f"Smart DFS destination preference: {smart_parlay_profiles['dfs']['recommended_target_label']}. "
                    f"{smart_parlay_profiles['dfs']['reason']}"
                )

            candidates = edge_df.copy()
            candidates = candidates[candidates["coverage_status"] == "Live"].copy()
            if parlay_focus_target == "builder":
                st.info("Parlay Lab jump is focused on the live builder settings and ticket draft below.")
            if parlay_candidate_pool == "Watchlist alerts":
                candidates = get_watchlist_alerts(candidates, sport_label)
            else:
                candidates = candidates[candidates["confidence"] >= min_confidence].copy()
                candidates = candidates.sort_values(["smart_score", "confidence", "edge"], ascending=False)

            st.info(
                (
                    f"Building from {len(candidates)} live candidate rows in the `{parlay_candidate_pool}` pool."
                    if not candidates.empty
                    else (
                        "Watchlist alert pool is empty right now. Adjust watchlist thresholds or switch back to all live edges."
                        if parlay_candidate_pool == "Watchlist alerts"
                        else "No live edges currently meet the selected confidence threshold. Lower the threshold or sync new market data."
                    )
                )
            )

            if parlay_candidate_pool == "Watchlist alerts" and candidates.empty:
                st.warning("No watchlist alerts currently meet the saved thresholds. Adjust the thresholds or use all live edges.")
            elif parlay_candidate_pool == "Watchlist alerts":
                st.caption("Drafting the ticket from the current watchlist alert pool.")

            if not allow_same_player:
                candidates = candidates.drop_duplicates(subset=["player"], keep="first")

            parlay_df = candidates.head(legs).copy()
            if saved_ticket_override_active and saved_ticket_source == "live_edges" and saved_ticket_payload:
                parlay_df = pd.DataFrame(saved_ticket_payload).copy()
            live_ticket_name = st.text_input("Live ticket name", value=f"{sport_label} Live Ticket", key="live_ticket_name")
            live_ticket_notes = st.text_input("Live ticket notes", key="live_ticket_notes")

            if parlay_df.empty or len(parlay_df) < legs:
                render_empty_state("Not enough live legs", "Loosen the confidence threshold, change the candidate pool, or allow multiple picks on the same player.", tone="warning")
            else:
                if saved_ticket_override_active and saved_ticket_source == "live_edges" and saved_ticket_payload:
                    st.caption("Currently rendering the selected saved live ticket inside Parlay Lab.")
                else:
                    st.caption("Live parlay mode only uses markets currently marked `Live` for this provider.")
                live_snapshot_col1, live_snapshot_col2, live_snapshot_col3, live_snapshot_col4 = st.columns(4)
                live_snapshot_col1.metric("Pool", parlay_candidate_pool)
                live_snapshot_col2.metric("Legs", str(legs))
                live_snapshot_col3.metric("Min confidence", f"{min_confidence}")
                live_snapshot_col4.metric("Same player", "Allowed" if allow_same_player else "Blocked")
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
                parlay_display = prefer_player_display(annotate_player_display(parlay_df))
                if "leg_rank" in parlay_display.columns:
                    parlay_display["leg_rank"] = parlay_display["leg_rank"].map(lambda value: f"Leg {int(value)}" if pd.notna(value) else "")
                if "market" in parlay_display.columns:
                    parlay_display["market"] = parlay_display["market"].map(prettify_market_label)
                parlay_display["bet"] = parlay_display.apply(format_bet_label, axis=1)
                parlay_display["leg_summary"] = parlay_display.apply(
                    lambda row: " | ".join(
                        part
                        for part in [
                            str(row.get("player", "")).strip(),
                            str(row.get("player_team", "")).strip(),
                            str(row.get("bet", "")).strip(),
                        ]
                        if part and part.lower() != "nan"
                    ),
                    axis=1,
                )
                parlay_display = parlay_display.rename(columns={"leg_summary": "summary"})
                live_parlay_columns = (
                    [
                        "leg_rank",
                        "summary",
                        "model_prob",
                        "edge",
                        "confidence",
                        "recommended_stake",
                        "sportsbook",
                        "coverage_status",
                    ]
                    if parlay_view_mode == "Compact"
                    else [
                        "leg_rank",
                        "event_id",
                        "player",
                        "player_team",
                        "market",
                        "pick",
                        "line",
                        "bet",
                        "sportsbook",
                        "projection",
                        "model_prob",
                        "implied_prob",
                        "edge",
                        "confidence",
                        "recommended_units",
                        "recommended_stake",
                        "coverage_status",
                    ]
                )
                st.dataframe(
                    style_coverage_table(compact_numeric_table(prettify_table_headers(parlay_display[
                        [
                            col for col in live_parlay_columns if col in parlay_display.columns
                        ]
                    ]))),
                    use_container_width=True,
                )
                st.markdown("#### Parlay Leg Audit")
                if parlay_focus_target == "audit":
                    st.info("Parlay Lab jump is focused on the live parlay leg audit below.")
                parlay_audit_candidates = parlay_df.sort_values(["smart_score", "smart_expected_win_rate", "edge"], ascending=False).copy() if "smart_score" in parlay_df.columns else parlay_df.copy()
                if parlay_audit_candidates.empty or "smart_audit_label" not in parlay_audit_candidates.columns:
                    st.caption("No smart audit is available for this parlay build yet.")
                else:
                    parlay_audit_pick = st.selectbox(
                        "Inspect a parlay leg",
                        parlay_audit_candidates["smart_audit_label"].tolist(),
                        key=f"parlay_leg_audit_{sport_label}",
                    )
                    parlay_audit_row = parlay_audit_candidates[parlay_audit_candidates["smart_audit_label"] == parlay_audit_pick].head(1)
                    if not parlay_audit_row.empty:
                        selected_parlay_audit = parlay_audit_row.iloc[0]
                        parlay_audit_col1, parlay_audit_col2, parlay_audit_col3, parlay_audit_col4 = st.columns(4)
                        parlay_audit_col1.metric("Smart score", f"{float(selected_parlay_audit.get('smart_score', 0.0) or 0.0):.1f}")
                        parlay_audit_col2.metric("Expected win %", f"{float(selected_parlay_audit.get('smart_expected_win_rate', 0.0) or 0.0) * 100:.1f}%")
                        parlay_audit_col3.metric("Tier", str(selected_parlay_audit.get("smart_tier") or "N/A"))
                        parlay_audit_col4.metric("History picks", f"{int(selected_parlay_audit.get('history_picks_used', 0) or 0)}")
                        st.caption(str(selected_parlay_audit.get("smart_summary") or ""))
                        parlay_history_compare = build_smart_history_comparison(selected_parlay_audit)
                        if not parlay_history_compare.empty:
                            st.markdown("##### Full History vs Recent Form")
                            parlay_history_display = parlay_history_compare.copy()
                            parlay_history_display["full_history"] = (pd.to_numeric(parlay_history_display["full_history"], errors="coerce") * 100).round(1)
                            parlay_history_display["recent_form"] = (pd.to_numeric(parlay_history_display["recent_form"], errors="coerce") * 100).round(1)
                            parlay_history_display["full_roi"] = pd.to_numeric(parlay_history_display["full_roi"], errors="coerce").round(2)
                            parlay_history_display["recent_roi"] = pd.to_numeric(parlay_history_display["recent_roi"], errors="coerce").round(2)
                            parlay_history_display = parlay_history_display.rename(
                                columns={
                                    "memory_type": "Memory Type",
                                    "full_history": "Full Hit Rate %",
                                    "recent_form": "Recent Hit Rate %",
                                    "full_sample": "Full Picks",
                                    "recent_sample": "Recent Picks",
                                    "full_roi": "Full Units/Pick",
                                    "recent_roi": "Recent Units/Pick",
                                }
                            )
                            st.dataframe(compact_numeric_table(parlay_history_display), use_container_width=True, hide_index=True)
                        st.dataframe(
                            compact_numeric_table(
                                build_smart_pick_audit(selected_parlay_audit).rename(
                                    columns={
                                        "component": "Score Component",
                                        "impact": "Impact",
                                        "detail": "Why",
                                    }
                                )
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
                if is_dfs:
                    if parlay_focus_target == "dfs_autoslip":
                        st.info("Parlay Lab jump is focused on the DFS auto-slip handoff below.")
                    selected_live_dfs_adapter = render_dfs_autoslip_panel(
                        card_df=parlay_df,
                        sport_label=sport_label,
                        source_label="Live DFS edges",
                        style_label=parlay_candidate_pool,
                        key_prefix=f"{sport_label}_live_dfs_autoslip",
                    )
        if st.button("Save Live Ticket", use_container_width=True):
                    ticket_id = save_ticket(
                        ticket_name=live_ticket_name,
                        sport_label=sport_label,
                        source="live_edges",
                        legs_df=parlay_df,
                        notes=live_ticket_notes or None,
                        metadata=(
                            {
                                "candidate_pool": parlay_candidate_pool,
                                "min_confidence": int(min_confidence),
                                "allow_same_player": bool(allow_same_player),
                                "smart_profile_mode": str(parlay_df.get("smart_profile_mode", pd.Series([""])).iloc[0]) if "smart_profile_mode" in parlay_df.columns and not parlay_df.empty else "",
                                "dfs_target_key": selected_live_dfs_adapter["key"],
                                "dfs_target_label": selected_live_dfs_adapter["label"],
                                "dfs_target_url": selected_live_dfs_adapter["launch_url"],
                            }
                            if is_dfs and selected_live_dfs_adapter
                            else {
                                "candidate_pool": parlay_candidate_pool,
                                "min_confidence": int(min_confidence),
                                "allow_same_player": bool(allow_same_player),
                                "smart_profile_mode": str(parlay_df.get("smart_profile_mode", pd.Series([""])).iloc[0]) if "smart_profile_mode" in parlay_df.columns and not parlay_df.empty else "",
                            }
                        ),
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
                        st.session_state["parlay_live_use_watchlist_alerts"] = False
    else:
        selected_demo_dfs_adapter = None
        st.caption("Demo mode uses the built-in synthetic prediction engine so you can iterate without live synced data.")
        demo_service = ResearchService()
        demo_bundle = demo_service.build_predictions(sport=sport_config["demo_key"])

        legs = st.slider(
            "Demo legs",
            min_value=2,
            max_value=6,
            key=demo_legs_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, demo_legs_session_key, "demo_legs"),
        )
        min_confidence = st.slider(
            "Demo minimum confidence",
            min_value=50,
            max_value=95,
            key=demo_min_conf_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, demo_min_conf_session_key, "demo_min_confidence"),
        )
        allow_same_team = st.checkbox(
            "Allow same-team demo legs",
            key=demo_same_team_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, demo_same_team_session_key, "demo_same_team"),
        )
        style = st.selectbox(
            "Parlay style",
            ["Safe", "Balanced", "Aggressive"],
            key=demo_style_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, demo_style_session_key, "demo_parlay_style"),
        )
        persist_preference_if_changed(sport_label, "demo_legs", legs, 3)
        persist_preference_if_changed(sport_label, "demo_min_confidence", min_confidence, 70)
        persist_preference_if_changed(sport_label, "demo_parlay_style", style, "Safe")
        persist_preference_if_changed(sport_label, "demo_same_team", allow_same_team, False)
        if parlay_focus_target == "smart_profiles":
            st.info("Parlay Lab jump is focused on Smart Profiles below.")
        render_smart_parlay_profile_panel(
            profile=smart_parlay_profiles["demo"],
            title="Smart Demo Parlay Profile",
            mode_label="Demo",
            current_values={
                "legs": legs,
                "min_confidence": min_confidence,
                "allow_overlap": allow_same_team,
            },
            apply_button_key=f"apply_demo_smart_profile_{sport_label}",
            apply_callback=lambda profile: {
                demo_legs_session_key: int(profile.get("recommended_legs", legs)),
                demo_min_conf_session_key: int(profile.get("recommended_min_confidence", min_confidence)),
                demo_style_session_key: str(profile.get("recommended_style", style)),
                demo_same_team_session_key: bool(profile.get("recommended_same_team", allow_same_team)),
            },
            pending_updates_key=demo_profile_pending_updates_key,
        )
        if is_dfs and str(smart_parlay_profiles["dfs"].get("recommended_target_label") or "").strip():
            st.caption(
                f"Smart DFS destination preference: {smart_parlay_profiles['dfs']['recommended_target_label']}. "
                f"{smart_parlay_profiles['dfs']['reason']}"
            )
        demo_ticket_name = st.text_input("Demo ticket name", value=f"{sport_label} Demo Ticket", key="demo_ticket_name")
        demo_ticket_notes = st.text_input("Demo ticket notes", key="demo_ticket_notes")

        if saved_ticket_override_active and saved_ticket_source == "demo_predictions" and saved_ticket_payload:
            parlay = pd.DataFrame(saved_ticket_payload).copy()
        else:
            parlay = build_parlay(
                demo_bundle.predictions,
                ParlaySettings(
                    legs=legs,
                    min_confidence=min_confidence,
                    allow_same_team=allow_same_team,
                    style=style,
                ),
            )

        total_demo_predictions = len(demo_bundle.predictions)
        if parlay_focus_target == "builder":
            st.info("Parlay Lab jump is focused on the demo builder settings and ticket draft below.")
        if saved_ticket_override_active and saved_ticket_source == "demo_predictions" and saved_ticket_payload:
            st.info(f"Rebuilding from the selected saved demo ticket using `{style}` as the current destination profile.")
        else:
            st.info(
                (
                    f"Building from {total_demo_predictions} demo predictions using the `{style}` profile."
                    if total_demo_predictions
                    else "No demo predictions are available for this sport right now."
                )
            )

        if parlay.empty:
            render_empty_state("No demo parlay met the filters", "Adjust the demo confidence, leg count, or style to widen the candidate pool.", tone="warning")
        else:
            demo_snapshot_col1, demo_snapshot_col2, demo_snapshot_col3, demo_snapshot_col4 = st.columns(4)
            demo_snapshot_col1.metric("Profile", style)
            demo_snapshot_col2.metric("Legs", str(legs))
            demo_snapshot_col3.metric("Min confidence", f"{min_confidence}")
            demo_snapshot_col4.metric("Same team", "Allowed" if allow_same_team else "Blocked")
            demo_parlay_display = prefer_player_display(annotate_player_display(parlay))
            if "leg_rank" in demo_parlay_display.columns:
                demo_parlay_display["leg_rank"] = demo_parlay_display["leg_rank"].map(lambda value: f"Leg {int(value)}" if pd.notna(value) else "")
            if "market" in demo_parlay_display.columns:
                demo_parlay_display["market"] = demo_parlay_display["market"].map(prettify_market_label)
            demo_parlay_display["bet"] = demo_parlay_display.apply(format_bet_label, axis=1)
            demo_parlay_display["leg_summary"] = demo_parlay_display.apply(
                lambda row: " | ".join(
                    part
                    for part in [
                        str(row.get("player", "")).strip(),
                        str(row.get("team", "")).strip(),
                        str(row.get("bet", "")).strip(),
                    ]
                    if part and part.lower() != "nan"
                ),
                axis=1,
            )
            demo_parlay_view_mode = st.radio(
                "Demo parlay table view",
                ["Compact", "Expanded"],
                horizontal=True,
                key=demo_parlay_view_session_key,
                on_change=persist_view_preference_from_session,
                args=(sport_label, demo_parlay_view_session_key, "demo_parlay_view_mode"),
            )
            demo_parlay_display = demo_parlay_display[
                [
                    col for col in (
                        [
                            "leg_rank",
                            "leg_summary",
                            "predicted_value",
                            "confidence",
                            "win_probability",
                        ]
                        if demo_parlay_view_mode == "Compact"
                        else [
                            "leg_rank",
                            "sport",
                            "player",
                            "market",
                            "pick",
                            "line",
                            "bet",
                            "predicted_value",
                            "confidence",
                            "win_probability",
                            "team",
                            "opponent",
                        ]
                    )
                    if col in demo_parlay_display.columns
                ]
            ].copy()
            demo_parlay_display = demo_parlay_display.rename(
                columns={
                    "predicted_value": "projection",
                    "win_probability": "model_prob",
                    "leg_summary": "summary",
                }
            )
            if "model_prob" in demo_parlay_display.columns:
                demo_parlay_display["model_prob"] = (pd.to_numeric(demo_parlay_display["model_prob"], errors="coerce") * 100).round(2)
            st.dataframe(compact_numeric_table(prettify_table_headers(demo_parlay_display)), use_container_width=True)
            if is_dfs:
                if parlay_focus_target == "dfs_autoslip":
                    st.info("Parlay Lab jump is focused on the DFS auto-slip handoff below.")
                selected_demo_dfs_adapter = render_dfs_autoslip_panel(
                    card_df=parlay,
                    sport_label=sport_label,
                    source_label="Demo DFS predictions",
                    style_label=style,
                    key_prefix=f"{sport_label}_demo_dfs_autoslip",
                )
            if st.button("Save Demo Ticket", use_container_width=True):
                ticket_id = save_ticket(
                    ticket_name=demo_ticket_name,
                    sport_label=sport_label,
                    source="demo_predictions",
                    legs_df=parlay,
                    notes=demo_ticket_notes or None,
                    metadata=(
                        {
                            "style": style,
                            "min_confidence": int(min_confidence),
                            "allow_same_team": bool(allow_same_team),
                            "dfs_target_key": selected_demo_dfs_adapter["key"],
                            "dfs_target_label": selected_demo_dfs_adapter["label"],
                            "dfs_target_url": selected_demo_dfs_adapter["launch_url"],
                        }
                        if is_dfs and selected_demo_dfs_adapter
                        else {
                            "style": style,
                            "min_confidence": int(min_confidence),
                            "allow_same_team": bool(allow_same_team),
                        }
                    ),
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
            key=history_player_suggestion_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, history_player_suggestion_session_key, "history_player_suggestion"),
        )
        persist_preference_if_changed(sport_label, "history_player_suggestion", selected_player, "")
        if st.button("Use Player Suggestion", use_container_width=True):
            st.session_state["history_player_input"] = selected_player

    with quick_fill_col2:
        selected_market = st.selectbox(
            "Quick-fill Market",
            [""] + history_suggestions["markets"],
            key=history_market_suggestion_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, history_market_suggestion_session_key, "history_market_suggestion"),
        )
        persist_preference_if_changed(sport_label, "history_market_suggestion", selected_market, "")
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
            history_display = prefer_player_display(annotate_player_display(history.copy()))
            if "market" in history_display.columns:
                history_display["market"] = history_display["market"].map(prettify_market_label)
            if {"pick", "line"}.intersection(history_display.columns):
                history_display["bet"] = history_display.apply(format_bet_label, axis=1)
            history_display["summary"] = history_display.apply(
                lambda row: " | ".join(
                    part
                    for part in [
                        str(row.get("player", "")).strip(),
                        str(row.get("player_team", row.get("team", ""))).strip(),
                        str(row.get("book", row.get("sportsbook", ""))).strip(),
                        str(row.get("bet", "")).strip(),
                    ]
                    if part and part.lower() != "nan"
                ),
                axis=1,
            )
            history_columns = [
                col
                for col in [
                    "summary",
                    "market",
                    "price",
                    "line",
                    "side",
                    "pulled_at",
                    "event_id",
                ]
                if col in history_display.columns
            ]
            st.dataframe(compact_numeric_table(prettify_table_headers(history_display[history_columns])), use_container_width=True)

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
    focus_target = st.session_state.get("dashboard_focus_target")
    results_focus_target = st.session_state.get("results_grading_section_focus_target")
    if focus_target == "results_grading":
        st.success("Notification focus is set to Results & Grading. Review unresolved picks, saved tickets, and recent settlement status here.")
    elif focus_target == "bankroll_journal":
        st.success("Notification focus is set to Bankroll Journal. Review open manual entries and settlement tasks below.")
    tracked_df = get_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    unresolved_tracked_df = get_unresolved_tracked_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    graded_df = get_graded_picks(live_sport_keys) if live_sport_keys else pd.DataFrame()
    smart_learning_tables = build_smart_learning_tables(graded_df)
    results_df = get_prop_results(live_sport_keys) if live_sport_keys else pd.DataFrame()
    auto_settle_scope = ",".join(live_sport_keys) if live_sport_keys else ""
    auto_settle_payload = get_sync_payload("sportsgameodds_auto_settle", auto_settle_scope) if auto_settle_scope else {}
    journal_df = get_journal_entries(sport_label)
    bankroll_summary = build_bankroll_summary(journal_df, bankroll_amount)
    bankroll_kpis = build_bankroll_kpis(journal_df, bankroll_amount)
    ticket_summary_df = get_ticket_summary_with_grades(sport_label)
    results_status_rows = [
        {
            "Workflow": "Tracked picks",
            "Status": "Ready" if not tracked_df.empty else "Pending",
            "Detail": f"{len(tracked_df)} tracked" if not tracked_df.empty else "Save live edges for grading to begin this queue.",
            "ActionLabel": "Open Edge Scanner" if tracked_df.empty else "Open Results & Grading",
            "ActionTarget": "edge_scanner" if tracked_df.empty else "results_grading",
            "ActionSectionTarget": "ungraded_tracked_picks" if not tracked_df.empty else "",
        },
        {
            "Workflow": "Settlement queue",
            "Status": "Needs action" if not unresolved_tracked_df.empty else "Clear",
            "Detail": (
                f"{len(unresolved_tracked_df)} open tracked picks waiting for grading or auto-settle."
                if not unresolved_tracked_df.empty
                else "No tracked picks are currently waiting for settlement."
            ),
            "ActionLabel": "Review Settlements" if not unresolved_tracked_df.empty else "Stay Clear",
            "ActionTarget": "results_grading",
            "ActionSectionTarget": "enter_settled_result",
        },
        {
            "Workflow": "Saved tickets",
            "Status": "Ready" if not ticket_summary_df.empty else "Pending",
            "Detail": (
                f"{len(ticket_summary_df)} saved tickets available for review."
                if not ticket_summary_df.empty
                else "Save a ticket from Parlay Lab to unlock ticket comparison and grading views."
            ),
            "ActionLabel": "Open Results & Grading" if not ticket_summary_df.empty else "Open Parlay Lab",
            "ActionTarget": "results_grading" if not ticket_summary_df.empty else "parlay_lab",
            "ActionSectionTarget": "saved_tickets" if not ticket_summary_df.empty else "",
        },
        {
            "Workflow": "Bankroll journal",
            "Status": "Ready" if not journal_df.empty else "Pending",
            "Detail": (
                f"{len(journal_df)} journal entries available for bankroll tracking."
                if not journal_df.empty
                else "Log a manual bet or save a tracked ticket with bankroll tracking to start this journal."
            ),
            "ActionLabel": "Open Bankroll Journal",
            "ActionTarget": "bankroll_journal",
            "ActionSectionTarget": "bankroll_journal",
        },
    ]

    st.markdown("### Workflow Status")
    st.caption("Use either the workflow jump button or the action button on each card to move straight into the relevant queue or journal.")
    status_tones = (
        {
            "Ready": {"bg": "#0f2b1f", "fg": "#8ee3b7", "border": "#1f6b4f", "card": "#0f1722", "title": "#e5eef8", "body": "#a7b6c8"},
            "Pending": {"bg": "#33270f", "fg": "#f5d27a", "border": "#7c5a18", "card": "#0f1722", "title": "#e5eef8", "body": "#a7b6c8"},
            "Needs action": {"bg": "#33161a", "fg": "#ff9d9d", "border": "#8b3a45", "card": "#0f1722", "title": "#e5eef8", "body": "#a7b6c8"},
            "Clear": {"bg": "#10233d", "fg": "#8ab4ff", "border": "#315b9b", "card": "#0f1722", "title": "#e5eef8", "body": "#a7b6c8"},
        }
        if theme_mode == "Dark"
        else {
            "Ready": {"bg": "#e8f7ef", "fg": "#157347", "border": "#b7e4c7", "card": "#ffffffcc", "title": "#17324d", "body": "#526273"},
            "Pending": {"bg": "#f7f4ea", "fg": "#8a6d1d", "border": "#ead9a7", "card": "#ffffffcc", "title": "#17324d", "body": "#526273"},
            "Needs action": {"bg": "#fbeaea", "fg": "#b42318", "border": "#f3c4c4", "card": "#ffffffcc", "title": "#17324d", "body": "#526273"},
            "Clear": {"bg": "#edf4ff", "fg": "#1d4ed8", "border": "#c7dbff", "card": "#ffffffcc", "title": "#17324d", "body": "#526273"},
        }
    )
    status_cols = st.columns(len(results_status_rows))
    for idx, row in enumerate(results_status_rows):
        tone = status_tones.get(row["Status"], status_tones["Pending"])
        if status_cols[idx].button(
            f"Jump to {row['Workflow']}",
            key=f"results_status_card_jump_{idx}",
            use_container_width=True,
        ):
            set_dashboard_focus(str(row["ActionTarget"]))
            if str(row.get("ActionTarget") or "") == "results_grading" and str(row.get("ActionSectionTarget") or "").strip():
                set_results_grading_focus(str(row["ActionSectionTarget"]))
            elif str(row.get("ActionTarget") or "") == "bankroll_journal":
                set_results_grading_focus("bankroll_journal")
            st.rerun()
        status_cols[idx].markdown(
            f"""
            <div style="
                border:1px solid {tone['border']};
                border-radius:18px;
                padding:16px 16px 14px 16px;
                background:{tone['card']};
                min-height:146px;
            ">
                <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
                    <div style="font-weight:700;color:{tone['title']};font-size:1rem;">{row['Workflow']}</div>
                    <span style="
                        background:{tone['bg']};
                        color:{tone['fg']};
                        border:1px solid {tone['border']};
                        border-radius:999px;
                        padding:4px 10px;
                        font-size:0.78rem;
                        font-weight:700;
                        white-space:nowrap;
                    ">{row['Status']}</span>
                </div>
                <div style="margin-top:12px;color:{tone['body']};font-size:0.93rem;line-height:1.45;">
                    {row['Detail']}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if row.get("ActionLabel"):
            if status_cols[idx].button(row["ActionLabel"], key=f"results_status_action_{idx}", use_container_width=True):
                set_dashboard_focus(str(row["ActionTarget"]))
                if str(row.get("ActionTarget") or "") == "results_grading" and str(row.get("ActionSectionTarget") or "").strip():
                    set_results_grading_focus(str(row["ActionSectionTarget"]))
                elif str(row.get("ActionTarget") or "") == "bankroll_journal":
                    set_results_grading_focus("bankroll_journal")
                st.rerun()




    def format_metric_or_na(value, formatter) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "N/A"
        try:
            return formatter(value)
        except Exception:
            return "N/A"

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Tracked Picks", f"{len(tracked_df)}")
    metric_col2.metric("Settled Results", f"{len(results_df)}")
    metric_col3.metric("Graded Picks", f"{len(graded_df)}")
    metric_col4.metric("Open Tracked Picks", f"{len(unresolved_tracked_df)}")

    source_summary_df = build_true_source_summary(graded_df)
    override_recommendation_title, override_recommendation_body = build_override_recommendation(source_summary_df)

    st.markdown("### Smart Pick Learning")
    if graded_df.empty:
        render_empty_state(
            "No smart-pick history yet",
            "Track and grade more picks so the smart engine can build reliable memory by market, sportsbook, and confidence band.",
            tone="info",
        )
    else:
        smart_summary_row = smart_learning_tables["summary"].iloc[0].to_dict() if not smart_learning_tables["summary"].empty else {}
        smart_weight_row = smart_learning_tables["weight_profile"].iloc[0].to_dict() if not smart_learning_tables.get("weight_profile", pd.DataFrame()).empty else {}
        auto_weight_profile = build_smart_weight_profile(graded_df)
        resolved_weight_profile = apply_smart_weight_overrides(auto_weight_profile, manual_smart_weight_overrides)
        smart_weight_row = resolved_weight_profile
        smart_metric_col1, smart_metric_col2, smart_metric_col3 = st.columns(3)
        smart_metric_col1.metric("History picks", f"{int(smart_summary_row.get('history_picks', 0) or 0)}")
        smart_metric_col2.metric("Historical hit rate", f"{float(smart_summary_row.get('overall_hit_rate', 0.0) or 0.0) * 100:.1f}%")
        smart_metric_col3.metric("Units per pick", f"{float(smart_summary_row.get('overall_roi_per_pick', 0.0) or 0.0):+.2f}")

        tune_col1, tune_col2, tune_col3, tune_col4 = st.columns(4)
        tune_col1.metric("Tuning mode", str(smart_weight_row.get("profile_mode", "default")).replace("_", " ").title())
        tune_col2.metric("Model weight", f"{float(smart_weight_row.get('model_score_weight', 0.42) or 0.42):.2f}")
        tune_col3.metric("History market weight", f"{float(smart_weight_row.get('history_market_weight', 0.36) or 0.36):.2f}")
        tune_col4.metric("Edge multiplier", f"{float(smart_weight_row.get('edge_multiplier', 1.45) or 1.45):.2f}")
        if smart_weight_row.get("profile_reason"):
            st.caption(str(smart_weight_row["profile_reason"]))

        with st.expander("Smart Engine Overrides", expanded=False):
            override_enabled = st.checkbox(
                "Use manual smart-engine overrides",
                key="smart_weights_override_enabled",
                on_change=persist_view_preference_from_session,
                args=("__app__", "smart_weights_override_enabled", "smart_weights_override_enabled"),
            )
            override_col1, override_col2 = st.columns(2)
            override_col3, override_col4 = st.columns(2)
            override_col1.slider(
                "Model weight",
                min_value=0.28,
                max_value=0.60,
                value=float(st.session_state.get("smart_model_weight", auto_weight_profile["model_score_weight"])),
                step=0.01,
                key="smart_model_weight",
                on_change=persist_view_preference_from_session,
                args=("__app__", "smart_model_weight", "smart_model_weight"),
                disabled=not override_enabled,
            )
            override_col2.slider(
                "Confidence weight",
                min_value=0.20,
                max_value=0.45,
                value=float(st.session_state.get("smart_confidence_weight", auto_weight_profile["confidence_score_weight"])),
                step=0.01,
                key="smart_confidence_weight",
                on_change=persist_view_preference_from_session,
                args=("__app__", "smart_confidence_weight", "smart_confidence_weight"),
                disabled=not override_enabled,
            )
            override_col3.slider(
                "Edge multiplier",
                min_value=1.00,
                max_value=2.10,
                value=float(st.session_state.get("smart_edge_multiplier", auto_weight_profile["edge_multiplier"])),
                step=0.05,
                key="smart_edge_multiplier",
                on_change=persist_view_preference_from_session,
                args=("__app__", "smart_edge_multiplier", "smart_edge_multiplier"),
                disabled=not override_enabled,
            )
            override_col4.slider(
                "Market history weight",
                min_value=0.15,
                max_value=0.60,
                value=float(st.session_state.get("smart_history_market_weight", auto_weight_profile["history_market_weight"])),
                step=0.01,
                key="smart_history_market_weight",
                on_change=persist_view_preference_from_session,
                args=("__app__", "smart_history_market_weight", "smart_history_market_weight"),
                disabled=not override_enabled,
            )
            compare_override_col1, compare_override_col2, compare_override_col3, compare_override_col4 = st.columns(4)
            compare_override_col1.metric("Auto model", f"{float(auto_weight_profile['model_score_weight']):.2f}")
            compare_override_col2.metric("Auto confidence", f"{float(auto_weight_profile['confidence_score_weight']):.2f}")
            compare_override_col3.metric("Auto edge", f"{float(auto_weight_profile['edge_multiplier']):.2f}")
            compare_override_col4.metric("Auto market history", f"{float(auto_weight_profile['history_market_weight']):.2f}")
            st.markdown("**Override Recommendation**")
            st.caption(f"{override_recommendation_title}: {override_recommendation_body}")
            promote_col1, promote_col2 = st.columns([1.2, 1.8])
            if promote_col1.button("Promote Recommended Mode", use_container_width=True):
                if apply_recommended_smart_mode(override_recommendation_title, auto_weight_profile):
                    st.success(f"Applied recommendation: {override_recommendation_title}.")
                    st.rerun()
                else:
                    st.info("The current recommendation is still observational, so the app is keeping your current smart-engine mode unchanged.")
            promote_col2.caption("This applies the current recommendation directly to the smart-engine mode. Auto disables manual overrides, while manual keeps your custom slider mix active.")
            if override_enabled:
                st.info(
                    "Manual overrides are active. Overview, Edge Scanner, and Parlay Lab will all use your custom smart-engine mix until you turn this off."
                )
            else:
                st.caption(
                    f"Auto-tuned profile currently resolves to model {float(resolved_weight_profile['model_score_weight']):.2f}, "
                    f"confidence {float(resolved_weight_profile['confidence_score_weight']):.2f}, "
                    f"edge {float(resolved_weight_profile['edge_multiplier']):.2f}, "
                    f"market history {float(resolved_weight_profile['history_market_weight']):.2f}."
                )

        recent_signal_col1, recent_signal_col2 = st.columns(2)
        recent_signal_col1.metric("Recent market signal", f"{float(smart_weight_row.get('recent_market_signal', 0.0) or 0.0):.3f}")
        recent_signal_col2.metric("Recent sportsbook signal", f"{float(smart_weight_row.get('recent_sportsbook_signal', 0.0) or 0.0):.3f}")
        st.caption("Recent-form memory is built from the latest settled graded sample, so the smart engine can react to short-term hot and cold pockets without abandoning full-history memory.")

        smart_tab1, smart_tab2, smart_tab3, smart_tab4, smart_tab5 = st.tabs(
            ["Best Markets", "Recent Markets", "Best Sportsbooks", "Recent Sportsbooks", "Confidence Memory"]
        )

        with smart_tab1:
            market_learning = smart_learning_tables["market_summary"]
            if market_learning.empty:
                st.caption("No market-specific history yet.")
            else:
                market_learning_display = build_smart_learning_display(
                    market_learning.head(12),
                    rename_map={
                        "market": "Market",
                        "market_picks": "Tracked Picks",
                        "market_hit_rate": "Hit Rate %",
                        "market_roi_per_pick": "Units Per Pick",
                        "market_avg_model_prob": "Avg Model %",
                        "market_avg_confidence": "Avg Confidence",
                    },
                    percent_columns=["market_hit_rate", "market_avg_model_prob"],
                    value_columns=["market_roi_per_pick", "market_avg_confidence"],
                )
                if "Market" in market_learning_display.columns:
                    market_learning_display["Market"] = market_learning_display["Market"].map(prettify_market_label)
                st.dataframe(market_learning_display, use_container_width=True, hide_index=True)

        with smart_tab2:
            recent_market_learning = smart_learning_tables.get("recent_market_summary", pd.DataFrame())
            if recent_market_learning.empty:
                st.caption("No recent market-form sample yet.")
            else:
                recent_market_display = build_smart_learning_display(
                    recent_market_learning.head(10),
                    rename_map={
                        "market": "Market",
                        "recent_market_picks": "Recent Picks",
                        "recent_market_hit_rate": "Recent Hit Rate %",
                        "recent_market_roi_per_pick": "Recent Units Per Pick",
                        "recent_market_avg_model_prob": "Recent Avg Model %",
                        "recent_market_avg_confidence": "Recent Avg Confidence",
                    },
                    percent_columns=["recent_market_hit_rate", "recent_market_avg_model_prob"],
                    value_columns=["recent_market_roi_per_pick", "recent_market_avg_confidence"],
                )
                if "Market" in recent_market_display.columns:
                    recent_market_display["Market"] = recent_market_display["Market"].map(prettify_market_label)
                st.dataframe(recent_market_display, use_container_width=True, hide_index=True)

        with smart_tab3:
            sportsbook_learning = smart_learning_tables["sportsbook_summary"]
            if sportsbook_learning.empty:
                st.caption("No sportsbook-specific history yet.")
            else:
                sportsbook_learning_display = build_smart_learning_display(
                    sportsbook_learning.head(12),
                    rename_map={
                        "sportsbook": "Sportsbook",
                        "sportsbook_picks": "Tracked Picks",
                        "sportsbook_hit_rate": "Hit Rate %",
                        "sportsbook_roi_per_pick": "Units Per Pick",
                        "sportsbook_avg_model_prob": "Avg Model %",
                        "sportsbook_avg_confidence": "Avg Confidence",
                    },
                    percent_columns=["sportsbook_hit_rate", "sportsbook_avg_model_prob"],
                    value_columns=["sportsbook_roi_per_pick", "sportsbook_avg_confidence"],
                )
                st.dataframe(sportsbook_learning_display, use_container_width=True, hide_index=True)

        with smart_tab4:
            recent_sportsbook_learning = smart_learning_tables.get("recent_sportsbook_summary", pd.DataFrame())
            if recent_sportsbook_learning.empty:
                st.caption("No recent sportsbook-form sample yet.")
            else:
                recent_sportsbook_display = build_smart_learning_display(
                    recent_sportsbook_learning.head(10),
                    rename_map={
                        "sportsbook": "Sportsbook",
                        "recent_sportsbook_picks": "Recent Picks",
                        "recent_sportsbook_hit_rate": "Recent Hit Rate %",
                        "recent_sportsbook_roi_per_pick": "Recent Units Per Pick",
                        "recent_sportsbook_avg_model_prob": "Recent Avg Model %",
                        "recent_sportsbook_avg_confidence": "Recent Avg Confidence",
                    },
                    percent_columns=["recent_sportsbook_hit_rate", "recent_sportsbook_avg_model_prob"],
                    value_columns=["recent_sportsbook_roi_per_pick", "recent_sportsbook_avg_confidence"],
                )
                st.dataframe(recent_sportsbook_display, use_container_width=True, hide_index=True)

        with smart_tab5:
            confidence_learning = smart_learning_tables["confidence_summary"]
            if confidence_learning.empty:
                st.caption("No confidence-band history yet.")
            else:
                confidence_learning_display = build_smart_learning_display(
                    confidence_learning.head(12),
                    rename_map={
                        "confidence_bucket": "Confidence Band",
                        "confidence_bucket_picks": "Tracked Picks",
                        "confidence_bucket_hit_rate": "Hit Rate %",
                        "confidence_bucket_roi_per_pick": "Units Per Pick",
                        "confidence_bucket_avg_model_prob": "Avg Model %",
                        "confidence_bucket_avg_confidence": "Avg Confidence",
                    },
                    percent_columns=["confidence_bucket_hit_rate", "confidence_bucket_avg_model_prob"],
                    value_columns=["confidence_bucket_roi_per_pick", "confidence_bucket_avg_confidence"],
                )
                st.dataframe(confidence_learning_display, use_container_width=True, hide_index=True)

    st.markdown("### Source Performance")
    if source_summary_df.empty:
        render_empty_state(
            "No source comparison yet",
            "Grade picks from different workflows to compare Smart Pick Engine performance against the legacy edge workflow.",
            tone="info",
        )
    else:
        source_summary_display = source_summary_df.copy()
        source_summary_display["source"] = source_summary_display["source"].map(format_source_label)
        source_summary_display["hit_rate"] = (source_summary_display["hit_rate"] * 100).round(1)
        source_summary_display["avg_model_prob"] = (source_summary_display["avg_model_prob"] * 100).round(1)
        source_summary_display["avg_edge"] = (source_summary_display["avg_edge"] * 100).round(1)
        source_summary_display["avg_confidence"] = source_summary_display["avg_confidence"].round(1)
        source_summary_display["profit_units"] = source_summary_display["profit_units"].round(2)
        source_summary_display["roi_per_pick"] = source_summary_display["roi_per_pick"].round(2)
        source_summary_display = source_summary_display.rename(
            columns={
                "source": "Workflow Source",
                "picks": "Tracked Picks",
                "hit_rate": "Hit Rate %",
                "avg_model_prob": "Avg Model %",
                "avg_edge": "Avg Edge %",
                "avg_confidence": "Avg Confidence",
                "profit_units": "Profit Units",
                "roi_per_pick": "Units Per Pick",
            }
        )
        st.dataframe(compact_numeric_table(source_summary_display), use_container_width=True, hide_index=True)
        snapshot_json = build_experiment_snapshot_payload(
            graded_df=graded_df,
            source_summary_df=source_summary_df,
            auto_weight_profile=auto_weight_profile if "auto_weight_profile" in locals() else build_smart_weight_profile(graded_df),
            resolved_weight_profile=resolved_weight_profile if "resolved_weight_profile" in locals() else apply_smart_weight_overrides(build_smart_weight_profile(graded_df), manual_smart_weight_overrides),
            recommendation_title=override_recommendation_title,
            recommendation_body=override_recommendation_body,
        )
        source_export_col1, source_export_col2 = st.columns(2)
        source_export_col1.download_button(
            "Download Experiment Snapshot JSON",
            data=snapshot_json,
            file_name=f"{sport_label.lower()}_smart_experiment_snapshot.json",
            mime="application/json",
            use_container_width=True,
        )
        source_export_col2.download_button(
            "Download Source Comparison CSV",
            data=source_summary_df.to_csv(index=False),
            file_name=f"{sport_label.lower()}_source_performance.csv",
            mime="text/csv",
            use_container_width=True,
        )

        auto_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_auto"].head(1)
        manual_row = source_summary_df[source_summary_df["source"] == "smart_pick_engine_manual"].head(1)
        legacy_row = source_summary_df[source_summary_df["source"] == "edge_scanner"].head(1)
        smart_row = source_summary_df[source_summary_df["source"].isin(["smart_pick_engine_auto", "smart_pick_engine_manual", "smart_pick_engine"])].head(1)
        if not smart_row.empty and not legacy_row.empty:
            smart_row = smart_row.iloc[0]
            legacy_row = legacy_row.iloc[0]
            compare_col1, compare_col2, compare_col3, compare_col4 = st.columns(4)
            compare_col1.metric(
                "Smart Hit Rate Lift",
                f"{(float(smart_row['hit_rate']) - float(legacy_row['hit_rate'])) * 100:+.1f} pts",
            )
            compare_col2.metric(
                "Smart Unit Lift",
                f"{float(smart_row['profit_units']) - float(legacy_row['profit_units']):+.2f}u",
            )
            compare_col3.metric(
                "Smart Units/Pick Lift",
                f"{float(smart_row['roi_per_pick']) - float(legacy_row['roi_per_pick']):+.2f}",
            )
            compare_col4.metric(
                "Sample Size",
                f"{int(smart_row['picks'])} vs {int(legacy_row['picks'])}",
            )
            st.caption(
                "This comparison uses graded picks only. As more smart-ranked picks settle, the signal here will become more reliable."
            )
        if not auto_row.empty and not manual_row.empty:
            auto_row = auto_row.iloc[0]
            manual_row = manual_row.iloc[0]
            st.markdown("#### Auto vs Manual Smart Testing")
            compare_mode_col1, compare_mode_col2, compare_mode_col3, compare_mode_col4 = st.columns(4)
            compare_mode_col1.metric(
                "Manual Hit Rate Lift",
                f"{(float(manual_row['hit_rate']) - float(auto_row['hit_rate'])) * 100:+.1f} pts",
            )
            compare_mode_col2.metric(
                "Manual Units/Pick Lift",
                f"{float(manual_row['roi_per_pick']) - float(auto_row['roi_per_pick']):+.2f}",
            )
            compare_mode_col3.metric(
                "Manual Unit Lift",
                f"{float(manual_row['profit_units']) - float(auto_row['profit_units']):+.2f}u",
            )
            compare_mode_col4.metric(
                "Sample Size",
                f"{int(manual_row['picks'])} vs {int(auto_row['picks'])}",
            )
            st.caption("Use this section to compare whether your manual smart-engine mix is actually outperforming the auto-tuned profile.")

        experiment_log = graded_df[graded_df["source"].isin(["smart_pick_engine_auto", "smart_pick_engine_manual", "edge_scanner"])].copy() if "source" in graded_df.columns else pd.DataFrame()
        if not experiment_log.empty:
            st.markdown("#### Experiment Log")
            experiment_log = prefer_player_display(annotate_player_display(experiment_log.copy()))
            experiment_log["source"] = experiment_log["source"].map(format_source_label)
            if {"pick", "line"}.intersection(experiment_log.columns):
                experiment_log["bet"] = experiment_log.apply(format_bet_label, axis=1)
            experiment_log["summary"] = experiment_log.apply(
                lambda row: " | ".join(
                    part
                    for part in [
                        str(row.get("source", "")).strip(),
                        str(row.get("player", "")).strip(),
                        str(row.get("bet", "")).strip(),
                    ]
                    if part and part.lower() != "nan"
                ),
                axis=1,
            )
            if "model_prob" in experiment_log.columns:
                experiment_log["model_prob"] = (pd.to_numeric(experiment_log["model_prob"], errors="coerce") * 100).round(1)
            if "edge" in experiment_log.columns:
                experiment_log["edge"] = (pd.to_numeric(experiment_log["edge"], errors="coerce") * 100).round(1)
            if "profit_units" in experiment_log.columns:
                experiment_log["profit_units"] = pd.to_numeric(experiment_log["profit_units"], errors="coerce").round(2)
            experiment_log_columns = [
                col
                for col in ["resolved_at", "summary", "grade", "profit_units", "model_prob", "edge", "confidence"]
                if col in experiment_log.columns
            ]
            st.dataframe(
                compact_numeric_table(experiment_log[experiment_log_columns].sort_values("resolved_at", ascending=False).head(40)),
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Download Experiment Log CSV",
                data=experiment_log.sort_values("resolved_at", ascending=False).to_csv(index=False),
                file_name=f"{sport_label.lower()}_experiment_log.csv",
                mime="text/csv",
                use_container_width=True,
            )

        cumulative_source_profit, rolling_source_hit_rate = build_true_source_timeseries(graded_df)
        trend_tab1, trend_tab2 = st.tabs(["Cumulative Units", "Rolling Hit Rate"])
        with trend_tab1:
            if cumulative_source_profit.empty:
                st.caption("Not enough resolved source history yet for a cumulative trend chart.")
            else:
                cumulative_chart = cumulative_source_profit.copy()
                cumulative_chart.columns = [format_source_label(str(col)) for col in cumulative_chart.columns]
                st.line_chart(cumulative_chart)
        with trend_tab2:
            if rolling_source_hit_rate.empty:
                st.caption("Not enough resolved source history yet for a rolling hit-rate chart.")
            else:
                rolling_chart = rolling_source_hit_rate.copy()
                rolling_chart.columns = [format_source_label(str(col)) for col in rolling_chart.columns]
                st.line_chart(rolling_chart)
                st.caption("Rolling hit rate uses the last 10 graded picks for each source.")

    weekly_review = build_weekly_model_review(graded_df)
    monthly_review = build_monthly_model_review(graded_df)
    recommendation_cards = build_model_recommendation_cards(weekly_review, monthly_review, sport_label=sport_label)
    review_action_checklist = build_review_action_checklist(weekly_review, monthly_review)
    render_recommendation_cards(recommendation_cards, "Recommendation Cards")
    render_review_action_checklist(
        checklist=review_action_checklist,
        sport_label=sport_label,
        live_legs_session_key=live_legs_session_key,
        live_min_conf_session_key=live_min_conf_session_key,
        live_same_player_session_key=live_same_player_session_key,
        demo_style_session_key=demo_style_session_key,
        demo_same_team_session_key=demo_same_team_session_key,
    )
    render_period_model_review(weekly_review, "Weekly Model Review")
    render_period_model_review(monthly_review, "Monthly Model Review")
    weekly_review_json = json.dumps(
        {
            "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
            "sport_label": sport_label,
            "recommendation_cards": recommendation_cards,
            "action_checklist": review_action_checklist,
            "weekly_review": {
                **weekly_review,
                "source_breakdown": weekly_review.get("source_breakdown", pd.DataFrame()).to_dict(orient="records")
                if isinstance(weekly_review.get("source_breakdown"), pd.DataFrame)
                else weekly_review.get("source_breakdown"),
                "market_breakdown": weekly_review.get("market_breakdown", pd.DataFrame()).to_dict(orient="records")
                if isinstance(weekly_review.get("market_breakdown"), pd.DataFrame)
                else weekly_review.get("market_breakdown"),
            },
            "monthly_review": {
                **monthly_review,
                "source_breakdown": monthly_review.get("source_breakdown", pd.DataFrame()).to_dict(orient="records")
                if isinstance(monthly_review.get("source_breakdown"), pd.DataFrame)
                else monthly_review.get("source_breakdown"),
                "market_breakdown": monthly_review.get("market_breakdown", pd.DataFrame()).to_dict(orient="records")
                if isinstance(monthly_review.get("market_breakdown"), pd.DataFrame)
                else monthly_review.get("market_breakdown"),
            },
        },
        indent=2,
        default=str,
    )
    st.download_button(
        "Download Weekly Review JSON",
        data=weekly_review_json,
        file_name=f"{sport_label.lower()}_weekly_model_review.json",
        mime="application/json",
        use_container_width=True,
    )

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
    if results_focus_target == "bankroll_journal":
        st.info("Workflow jump is focused on Bankroll Journal below.")
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
        render_empty_state(
            "No bankroll journal entries yet",
            "Log a manual bet or save a ticket with bankroll tracking to start building this journal. Journal summaries, settlement actions, and bankroll trend details will appear here after your first entry.",
            tone="neutral",
        )
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
        journal_display = journal_df.copy()
        journal_display["summary"] = journal_display.apply(
            lambda row: " | ".join(
                part
                for part in [
                    str(row.get("label", "")).strip(),
                    str(row.get("entry_type", "")).strip().replace("_", " ").title(),
                    str(row.get("status", "")).strip().title(),
                ]
                if part and part.lower() != "nan"
            ),
            axis=1,
        )
        journal_columns = [
            col
            for col in [
                "summary",
                "stake_dollars",
                "stake_units",
                "realized_profit_dollars",
                "suggested_stake_dollars",
                "ticket_id",
                "resolved_at",
                "created_at",
            ]
            if col in journal_display.columns
        ]
        st.dataframe(compact_numeric_table(prettify_table_headers(journal_display[journal_columns])), use_container_width=True)
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
    if results_focus_target == "enter_settled_result":
        st.info("Workflow jump is focused on the settlement queue. Enter or sync results below.")
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
    if results_focus_target == "ungraded_tracked_picks":
        st.info("Workflow jump is focused on tracked picks waiting for review.")
    if ungraded_df.empty:
        render_empty_state(
            "No ungraded tracked picks",
            "Save live edges for grading or wait for new tracked picks to settle into this queue. Resolution controls and tracked-pick review rows will appear here after your first tracked pick.",
            tone="neutral",
        )
    else:
        ungraded_display = prefer_player_display(annotate_player_display(ungraded_df.head(100).copy()))
        if "market" in ungraded_display.columns:
            ungraded_display["market"] = ungraded_display["market"].map(prettify_market_label)
        if {"pick", "line"}.intersection(ungraded_display.columns):
            ungraded_display["bet"] = ungraded_display.apply(format_bet_label, axis=1)
        ungraded_display["summary"] = ungraded_display.apply(
            lambda row: " | ".join(
                part
                for part in [
                    str(row.get("player", "")).strip(),
                    str(row.get("player_team", row.get("team", ""))).strip(),
                    str(row.get("bet", "")).strip(),
                ]
                if part and part.lower() != "nan"
            ),
            axis=1,
        )
        ungraded_columns = [
            col
            for col in [
                "summary",
                "sportsbook",
                "event_id",
                "tracked_at",
                "model_prob",
                "edge",
                "confidence",
            ]
            if col in ungraded_display.columns
        ]
        if "model_prob" in ungraded_display.columns:
            ungraded_display["model_prob"] = (pd.to_numeric(ungraded_display["model_prob"], errors="coerce") * 100).round(2)
        if "edge" in ungraded_display.columns:
            ungraded_display["edge"] = (pd.to_numeric(ungraded_display["edge"], errors="coerce") * 100).round(2)
        st.dataframe(compact_numeric_table(prettify_table_headers(ungraded_display[ungraded_columns])), use_container_width=True)
        st.download_button(
            "Export Ungraded Tracked Picks CSV",
            data=ungraded_df.to_csv(index=False),
            file_name=f"{sport_label.lower()}_tracked_picks.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown("### Graded Picks")
    if graded_df.empty:
        render_empty_state(
            "No graded picks yet",
            "Track live edges, auto-sync settled results, or enter manual results to start filling this history. Graded filters and sort controls will appear here after your first graded pick.",
            tone="neutral",
        )
    else:
        graded_filter_col1, graded_filter_col2, graded_filter_col3 = st.columns(3)
        graded_market_filter = graded_filter_col1.selectbox(
            "Graded market filter",
            [""] + sorted(graded_df["market"].dropna().astype(str).unique().tolist()),
            key=graded_market_filter_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, graded_market_filter_session_key, "graded_market_filter"),
        )
        graded_player_filter = graded_filter_col2.text_input("Graded player search", key="graded_player_filter")
        graded_sort_by = graded_filter_col3.selectbox(
            "Graded sort by",
            [col for col in ["resolved_at", "profit_units", "edge", "model_prob", "confidence"] if col in graded_df.columns],
            key=graded_sort_by_session_key,
            on_change=persist_view_preference_from_session,
            args=(sport_label, graded_sort_by_session_key, "graded_sort_by"),
        )
        persist_preference_if_changed(sport_label, "graded_market_filter", graded_market_filter, "")
        persist_preference_if_changed(sport_label, "graded_sort_by", graded_sort_by, "resolved_at")
        graded_df = filter_dataframe(
            graded_df,
            market_key=graded_market_filter,
            player_query=graded_player_filter,
            sort_by=graded_sort_by,
            ascending=False,
        )
        graded_display = prefer_player_display(annotate_player_display(graded_df.copy()))
        if "market" in graded_display.columns:
            graded_display["market"] = graded_display["market"].map(prettify_market_label)
        if {"pick", "line"}.intersection(graded_display.columns):
            graded_display["bet"] = graded_display.apply(format_bet_label, axis=1)
        graded_display["summary"] = graded_display.apply(
            lambda row: " | ".join(
                part
                for part in [
                    str(row.get("player", "")).strip(),
                    str(row.get("player_team", row.get("team", ""))).strip(),
                    str(row.get("bet", "")).strip(),
                ]
                if part and part.lower() != "nan"
            ),
            axis=1,
        )
        graded_columns = [
            col
            for col in [
                "summary",
                "sportsbook",
                "actual_value",
                "winning_side",
                "grade",
                "profit_units",
                "model_prob",
                "edge",
                "tracked_at",
                "resolved_at",
            ]
            if col in graded_display.columns
        ]
        if "model_prob" in graded_display.columns:
            graded_display["model_prob"] = (pd.to_numeric(graded_display["model_prob"], errors="coerce") * 100).round(2)
        if "edge" in graded_display.columns:
            graded_display["edge"] = (pd.to_numeric(graded_display["edge"], errors="coerce") * 100).round(2)
        if "profit_units" in graded_display.columns:
            graded_display["profit_units"] = pd.to_numeric(graded_display["profit_units"], errors="coerce").round(3)
        st.dataframe(compact_numeric_table(prettify_table_headers(graded_display[graded_columns].head(200))), use_container_width=True)
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
    if results_focus_target == "saved_tickets":
        st.info("Workflow jump is focused on saved tickets and ticket review.")
    ticket_summary_df = get_ticket_summary_with_grades(sport_label)
    if ticket_summary_df.empty:
        render_empty_state(
            "No saved tickets yet",
            "Save a ticket from Parlay Lab to start comparing, grading, and tracking live slips here. Ticket summaries, leg breakdowns, and model-vs-ticket comparison tools will appear here after your first saved ticket.",
            tone="neutral",
        )
    else:
        display_tickets = ticket_summary_df[
            [
                "ticket_id",
                "name",
                "source",
                "dfs_target_app",
                "leg_count",
                "avg_confidence",
                "avg_model_prob",
                "build_candidate_pool",
                "build_style",
                "build_min_confidence",
                "build_smart_profile_mode",
                "ticket_outcome_score",
                "ticket_profit_units",
                "ticket_missing_price_legs",
                "resolved_ratio",
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
        if "ticket_outcome_score" in display_tickets.columns:
            display_tickets["ticket_outcome_score"] = pd.to_numeric(display_tickets["ticket_outcome_score"], errors="coerce").round(2)
        if "ticket_profit_units" in display_tickets.columns:
            display_tickets["ticket_profit_units"] = pd.to_numeric(display_tickets["ticket_profit_units"], errors="coerce").round(2)
        if "resolved_ratio" in display_tickets.columns:
            display_tickets["resolved_ratio"] = (pd.to_numeric(display_tickets["resolved_ratio"], errors="coerce") * 100).round(1)
        if "source" in display_tickets.columns:
            display_tickets["source"] = display_tickets["source"].map(format_source_label)
        if "build_smart_profile_mode" in display_tickets.columns:
            display_tickets["build_smart_profile_mode"] = display_tickets["build_smart_profile_mode"].map(lambda value: str(value or "").replace("_", " ").title())
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
        selected_ticket_meta = ticket_summary_df[ticket_summary_df["ticket_id"] == selected_ticket_id].head(1)
        if not selected_ticket_meta.empty:
            ticket_row = selected_ticket_meta.iloc[0]
            dfs_adapter = get_dfs_adapter_by_key(str(ticket_row.get("dfs_target_key") or ""))
            snapshot_col1, snapshot_col2, snapshot_col3, snapshot_col4 = st.columns(4)
            snapshot_col1.metric("Ticket", str(ticket_row["name"]))
            snapshot_col2.metric("Source", str(ticket_row["source"]).replace("_", " ").title())
            snapshot_col3.metric("Legs", str(ticket_row["leg_count"]))
            snapshot_col4.metric("Status", str(ticket_row["ticket_status_live"]).replace("_", " ").title())

            snapshot_col5, snapshot_col6, snapshot_col7, snapshot_col8 = st.columns(4)
            snapshot_col5.metric("Avg confidence", f"{float(ticket_row['avg_confidence']):.1f}" if pd.notna(ticket_row["avg_confidence"]) else "N/A")
            snapshot_col6.metric("Avg model %", f"{float(ticket_row['avg_model_prob']) * 100:.2f}%" if pd.notna(ticket_row["avg_model_prob"]) else "N/A")
            snapshot_col7.metric("Resolved legs", str(ticket_row["resolved_legs"]))
            snapshot_col8.metric("Open legs", str(ticket_row["open_legs"]))
            outcome_col1, outcome_col2, outcome_col3 = st.columns(3)
            outcome_col1.metric(
                "Ticket outcome score",
                f"{float(ticket_row['ticket_outcome_score']):.2f}" if pd.notna(ticket_row.get("ticket_outcome_score")) else "N/A",
            )
            outcome_col2.metric(
                "Resolved %",
                f"{float(ticket_row['resolved_ratio']) * 100:.1f}%" if pd.notna(ticket_row.get("resolved_ratio")) else "N/A",
            )
            outcome_col3.metric(
                "Est. ticket units",
                f"{float(ticket_row['ticket_profit_units']):+.2f}u" if pd.notna(ticket_row.get("ticket_profit_units")) else "N/A",
            )
            if pd.notna(ticket_row.get("ticket_missing_price_legs")) and float(ticket_row.get("ticket_missing_price_legs") or 0) > 0:
                st.caption(
                    f"Estimated ticket units use stored leg prices when available. {int(float(ticket_row['ticket_missing_price_legs']))} leg(s) were missing price data, so even-money fallback pricing was used for those legs."
                )
            build_col1, build_col2, build_col3, build_col4 = st.columns(4)
            build_col1.metric("Candidate pool", str(ticket_row.get("build_candidate_pool") or "N/A"))
            build_col2.metric("Build style", str(ticket_row.get("build_style") or "N/A"))
            build_col3.metric(
                "Build min confidence",
                str(int(ticket_row["build_min_confidence"])) if pd.notna(ticket_row.get("build_min_confidence")) else "N/A",
            )
            build_col4.metric(
                "Profile mode",
                str(ticket_row.get("build_smart_profile_mode") or "N/A").replace("_", " ").title(),
            )
            build_col5, build_col6 = st.columns(2)
            build_col5.metric(
                "Same player",
                format_bool_build_setting(ticket_row.get("build_allow_same_player")),
            )
            build_col6.metric(
                "Same team",
                format_bool_build_setting(ticket_row.get("build_allow_same_team")),
            )
            if str(ticket_row.get("dfs_target_app") or "").strip():
                if dfs_adapter:
                    st.markdown(
                        f"""
                        <div style="
                            display:flex;
                            align-items:center;
                            gap:0.55rem;
                            margin:0.4rem 0 0.15rem;
                        ">
                            <span style="
                                display:inline-flex;
                                align-items:center;
                                justify-content:center;
                                width:1.8rem;
                                height:1.8rem;
                                border-radius:999px;
                                background:{dfs_adapter['accent']};
                                color:#f8fbff;
                                font-size:0.76rem;
                                font-weight:800;
                            ">{dfs_adapter['brand_mark']}</span>
                            <span style="font-size:0.92rem;color:#9fc4e8;">Saved DFS destination: {ticket_row['dfs_target_app']}</span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption(f"Saved DFS destination: {ticket_row['dfs_target_app']}")

        if not selected_legs.empty:
            saved_ticket_display = prefer_player_display(annotate_player_display(selected_legs.copy()))
            if "leg_rank" in saved_ticket_display.columns:
                saved_ticket_display["leg_rank"] = saved_ticket_display["leg_rank"].map(lambda value: f"Leg {int(value)}" if pd.notna(value) else "")
            if "market" in saved_ticket_display.columns:
                saved_ticket_display["market"] = saved_ticket_display["market"].map(prettify_market_label)
            if {"pick", "line"}.intersection(saved_ticket_display.columns):
                saved_ticket_display["bet"] = saved_ticket_display.apply(format_bet_label, axis=1)
            saved_ticket_display["summary"] = saved_ticket_display.apply(
                lambda row: " | ".join(
                    part
                    for part in [
                        str(row.get("player", "")).strip(),
                        str(row.get("player_team", row.get("team", ""))).strip(),
                        str(row.get("bet", "")).strip(),
                    ]
                    if part and part.lower() != "nan"
                ),
                axis=1,
            )
            compact_ticket_columns = [
                col
                for col in [
                    "leg_rank",
                    "summary",
                    "sportsbook",
                    "grade",
                    "actual_value",
                    "winning_side",
                    "profit_units",
                ]
                if col in saved_ticket_display.columns
            ]
            if compact_ticket_columns:
                st.dataframe(
                    compact_numeric_table(prettify_table_headers(saved_ticket_display[compact_ticket_columns])),
                    use_container_width=True,
                )
            else:
                st.dataframe(compact_numeric_table(saved_ticket_display), use_container_width=True)

            if st.button("Use This Saved Ticket In Parlay Lab", use_container_width=True, key=f"use_saved_ticket_{int(selected_ticket_id)}"):
                promote_saved_ticket_to_parlay_lab(
                    ticket_id=int(selected_ticket_id),
                    ticket_name=str(ticket_row.get("name") or f"Ticket {int(selected_ticket_id)}"),
                    ticket_row=ticket_row,
                    legs_df=selected_legs,
                )
                st.success("Sent this saved ticket back to Parlay Lab.")
                st.rerun()

            if ticket_looks_like_dfs(ticket_row, selected_legs):
                st.markdown("#### Rebuild For Another DFS App")
                render_dfs_autoslip_panel(
                    card_df=selected_legs,
                    sport_label=sport_label,
                    source_label=f"Saved ticket #{int(selected_ticket_id)}",
                    style_label=str(ticket_row.get("dfs_target_app") or ticket_row.get("source") or "Saved ticket"),
                    key_prefix=f"saved_ticket_{int(selected_ticket_id)}_dfs_autoslip",
                )

            if not selected_ticket_meta.empty:
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
                    comparison_col1.metric("Ticket Avg Confidence", format_metric_or_na(ticket_row.get("avg_confidence"), lambda value: f"{float(value):.1f}"))
                    comparison_col2.metric("Ticket Avg Model Prob", format_metric_or_na(ticket_row.get("avg_model_prob"), lambda value: f"{float(value) * 100:.2f}%"))
                    comparison_col3.metric("Benchmark Avg Confidence", format_metric_or_na(benchmark.get("benchmark_avg_confidence"), lambda value: f"{float(value):.1f}"))
                    comparison_col4.metric("Current Top-Leg Overlap", f"{overlap_count}/{int(ticket_row['leg_count'])}")

                    comparison_col5, comparison_col6, comparison_col7 = st.columns(3)
                    comparison_col5.metric(
                        "Benchmark Hit Rate",
                        format_metric_or_na(benchmark.get("benchmark_hit_rate"), lambda value: f"{float(value) * 100:.2f}%"),
                    )
                    comparison_col6.metric(
                        "Benchmark Profit Units",
                        format_metric_or_na(benchmark.get("benchmark_profit_units"), lambda value: f"{float(value):.2f}"),
                    )
                    comparison_col7.metric("Ticket Status", str(ticket_row["ticket_status_live"]))
                    ticket_review_cards = build_ticket_review_insights(ticket_row, benchmark, overlap_count, current_benchmark)
                    render_recommendation_cards(ticket_review_cards, "Ticket Review Insights")

                    if not ticket_legs_with_results.empty:
                        st.markdown("##### Ticket Leg Outcomes")
                        ticket_leg_outcomes_display = ticket_legs_with_results[
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
                        ].copy()
                        ticket_leg_outcomes_display["actual_value"] = ticket_leg_outcomes_display["actual_value"].map(
                            lambda value: format_pending_result_value(value, "Pending")
                        )
                        ticket_leg_outcomes_display["winning_side"] = ticket_leg_outcomes_display["winning_side"].map(
                            lambda value: format_pending_result_value(value, "Awaiting result")
                        )
                        st.dataframe(
                            compact_numeric_table(ticket_leg_outcomes_display),
                            use_container_width=True,
                        )

                    if not current_benchmark.empty:
                        st.markdown("##### Current Top Model Legs")
                        benchmark_display = prefer_player_display(annotate_player_display(current_benchmark))
                        benchmark_display = benchmark_display[
                            [
                                col for col in [
                                    "player",
                                    "player_team",
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
                                if col in benchmark_display.columns
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
    backtest_focus_target = st.session_state.get("backtest_section_focus_target", "")
    backtest_jump_col1, backtest_jump_col2 = st.columns(2)
    if backtest_jump_col1.button("Jump to True Results", key="backtest_jump_true_results", use_container_width=True):
        set_backtest_focus("true_results")
        st.rerun()
    if backtest_jump_col2.button("Jump to CLV Diagnostics", key="backtest_jump_clv", use_container_width=True):
        set_backtest_focus("clv_diagnostics")
        st.rerun()
    true_backtest_df = pd.DataFrame()
    clv_backtest_df = pd.DataFrame()

    if live_sport_keys:
        true_backtest_df = build_true_backtest(live_sport_keys)
        clv_backtest_df = build_clv_backtest(live_sport_keys)

    if true_backtest_df.empty and clv_backtest_df.empty:
        render_empty_state("Not enough backtest history yet", "Build more graded results or live line history before diagnostics can populate for this sport.", tone="info")
    else:
        if not true_backtest_df.empty:
            st.markdown("### True Results Backtest")
            if backtest_focus_target == "true_results":
                st.info("Backtest jump is focused on True Results diagnostics below.")
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
            if backtest_focus_target == "clv_diagnostics":
                st.info("Backtest jump is focused on CLV proxy diagnostics below.")
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
                display_calibration["prob_bucket"] = display_calibration["prob_bucket"].map(format_probability_bucket_label)
                display_calibration["avg_model_prob"] = (display_calibration["avg_model_prob"] * 100).round(2)
                display_calibration["clv_hit_rate"] = (display_calibration["clv_hit_rate"] * 100).round(2)
                display_calibration["avg_edge"] = (display_calibration["avg_edge"] * 100).round(2)
                display_calibration["avg_line_move"] = display_calibration["avg_line_move"].round(3)
                display_calibration = display_calibration.rename(
                    columns={
                        "prob_bucket": "Model Probability Range",
                        "picks": "Tracked Picks",
                        "avg_model_prob": "Avg Model %",
                        "clv_hit_rate": "CLV Hit %",
                        "avg_edge": "Avg Edge %",
                        "avg_line_move": "Avg Line Move",
                    }
                )
                st.dataframe(compact_numeric_table(display_calibration), use_container_width=True, hide_index=True)

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

