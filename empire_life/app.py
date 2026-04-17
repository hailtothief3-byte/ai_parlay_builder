from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from systems.choices import get_available_choices, process_choice
from systems.consequences import check_end_state, end_turn_update
from systems.events import resolve_event
from systems.game_state import init_game_state
from systems.player import apply_background_bonus, clamp_player
from systems.save_manager import delete_game, list_save_slots, load_game, save_game


st.set_page_config(page_title="Empire Life", layout="wide")

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(201, 153, 84, 0.10), transparent 30%),
            linear-gradient(180deg, #0f1115 0%, #171b22 100%);
        color: #f2ede3;
    }
    [data-testid="stHeader"] {
        background: rgba(10, 12, 16, 0.72);
        border-bottom: 1px solid rgba(255,255,255,0.06);
    }
    .block-container {
        padding-top: 2.25rem;
        padding-bottom: 3rem;
    }
    .stApp h1 {
        color: #f7f1e6;
    }
    .stApp h2, .stApp h3 {
        color: #eadfca;
    }
    .stApp p, .stApp li, .stApp label, .stApp span, .stApp div {
        color: #d7cfbf;
    }
    .stApp .stCaptionContainer, .stApp [data-testid="stCaptionContainer"] {
        color: #bcae97;
    }
    .stApp [data-baseweb="select"] > div,
    .stApp [data-baseweb="input"] > div {
        background: rgba(247, 241, 230, 0.96);
        color: #1b2230;
    }
    .stApp input, .stApp textarea {
        color: #1b2230 !important;
    }
    .stApp [data-baseweb="select"] svg {
        fill: #1b2230;
    }
    div[data-testid="stMetric"] {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 0.75rem;
    }
    .empire-panel {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 1rem 1.1rem;
        margin-bottom: 1rem;
    }
    .empire-hero {
        background:
            linear-gradient(135deg, rgba(213, 166, 90, 0.18), rgba(74, 107, 168, 0.08)),
            rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 24px;
        padding: 1.4rem 1.5rem;
        margin-bottom: 1.2rem;
        box-shadow: 0 24px 60px rgba(0,0,0,0.18);
    }
    .empire-hero h1 {
        margin: 0;
        font-size: 3rem;
        letter-spacing: -0.04em;
    }
    .empire-hero p {
        margin: 0.35rem 0 0;
        color: #d7cfbf;
        font-size: 1.05rem;
    }
    .empire-section-label {
        color: #bcae97;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        font-size: 0.76rem;
        margin-bottom: 0.35rem;
    }
    .empire-subtle {
        color: #a89d8b;
        font-size: 0.95rem;
    }
    .empire-story-card {
        background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.03));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 1rem 1.1rem;
        min-height: 100%;
    }
    .empire-start-grid {
        display: grid;
        grid-template-columns: 1.3fr 0.9fr;
        gap: 1rem;
        margin-bottom: 1rem;
    }
    .empire-tag-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.55rem;
        margin: 1rem 0 0.6rem;
    }
    .empire-tag {
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.08);
        color: #eadfca;
        border-radius: 999px;
        padding: 0.42rem 0.8rem;
        font-size: 0.84rem;
    }
    .empire-quote {
        font-size: 1.1rem;
        line-height: 1.6;
        color: #f7f1e6;
        margin-top: 0.4rem;
    }
    .empire-feature-list {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.7rem;
        margin-top: 1rem;
    }
    .empire-feature-item {
        background: rgba(255,255,255,0.035);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 0.8rem 0.9rem;
    }
    .empire-feature-item strong {
        display: block;
        color: #f0e5d3;
        margin-bottom: 0.2rem;
    }
    .empire-action-shell {
        background: rgba(255,255,255,0.025);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 22px;
        padding: 1rem 1rem 0.6rem;
        margin-bottom: 1rem;
    }
    .stButton > button {
        background: linear-gradient(180deg, #d5a65a 0%, #b9873f 100%);
        color: #151922 !important;
        border: none;
        font-weight: 600;
        min-height: 3.2rem;
        box-shadow: 0 10px 24px rgba(0,0,0,0.12);
    }
    .stButton > button:hover {
        background: linear-gradient(180deg, #e0b46a 0%, #c59248 100%);
        color: #10141b !important;
    }
    .stButton > button p,
    .stButton > button span,
    .stButton > button div {
        color: #151922 !important;
    }
    ul {
        padding-left: 1.25rem;
    }
    li {
        margin-bottom: 0.45rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def start_new_life(name: str, background: str, life_stage: str) -> None:
    game = st.session_state.game
    game["started"] = True
    game["player"]["name"] = name.strip() or "Player"
    game["player"]["background"] = background
    game["player"]["life_stage"] = life_stage
    game["player"]["age"] = 10 if life_stage == "Childhood" else 15
    apply_background_bonus(game, background)
    game["log"].insert(0, f"Your story begins in {life_stage.lower()}.")


def render_start_screen() -> None:
    st.markdown(
        """
        <div class="empire-hero">
            <div class="empire-section-label">Consequence-Driven Life Simulator</div>
            <h1>Empire Life</h1>
            <p>A game about pressure, temptation, family, ambition, and who you become when life keeps testing you.</p>
            <div class="empire-tag-row">
                <div class="empire-tag">Youth to adulthood</div>
                <div class="empire-tag">Neighborhood pressure</div>
                <div class="empire-tag">Family consequences</div>
                <div class="empire-tag">Risk versus structure</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="empire-start-grid">
            <div class="empire-story-card">
                <div class="empire-section-label">Premise</div>
                <div class="empire-quote">Build a life, chase an image, protect your people, or lose yourself trying.</div>
                <div class="empire-feature-list">
                    <div class="empire-feature-item">
                        <strong>Childhood matters</strong>
                        Small early choices shape later options.
                    </div>
                    <div class="empire-feature-item">
                        <strong>Temptation feels real</strong>
                        The easy path pays fast and punishes late.
                    </div>
                    <div class="empire-feature-item">
                        <strong>Family is not background</strong>
                        Trust, neglect, and responsibility all count.
                    </div>
                    <div class="empire-feature-item">
                        <strong>No single right path</strong>
                        Structured, risky, family, and community runs all play differently.
                    </div>
                </div>
            </div>
            <div class="empire-story-card">
                <div class="empire-section-label">Recommended Starts</div>
                <p><strong>Childhood:</strong> best for the full experience and strongest long-term consequences.</p>
                <p><strong>Teen:</strong> faster, messier, and more volatile for quick playtests.</p>
                <p><strong>Supportive Family:</strong> easiest lane to learn the systems.</p>
                <p><strong>Street Exposure:</strong> best if you want immediate pressure and more dangerous momentum.</p>
                <p class="empire-subtle">This prototype is strongest when you lean into a path instead of clicking randomly.</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.subheader("Create Your Character")

    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Name", placeholder="Enter character name")
        life_stage = st.selectbox("Start life stage", ["Childhood", "Teen"])
    with col2:
        background = st.selectbox(
            "Family background",
            [
                "Balanced",
                "Struggling Household",
                "Street Exposure",
                "Supportive Family",
            ],
        )

    intro_left, intro_right = st.columns([1.05, 0.95])
    with intro_left:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">Prototype Focus</div>', unsafe_allow_html=True)
        st.markdown(
            """
            ### What This Build Is Testing
            - Youth-to-adulthood progression
            - Family pressure and emotional strain
            - Temptation versus discipline
            - Honest and risky paths
            - Delayed consequences that change future options
            """
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with intro_right:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">Design Intent</div>', unsafe_allow_html=True)
        st.write(
            "This prototype is strongest when the player feels real tradeoffs: safety versus image, structure versus chaos, and peace versus immediate gratification."
        )
        st.caption("Start in childhood for the fullest arc. Start in teen years for a faster, more volatile run.")
        st.markdown("</div>", unsafe_allow_html=True)

    saves = list_save_slots()
    if saves:
        st.markdown('<div class="empire-panel">', unsafe_allow_html=True)
        st.subheader("Continue a Saved Run")
        selected = st.selectbox("Saved slot", saves, key="load_slot")
        load_cols = st.columns(2)
        with load_cols[0]:
            if st.button("Load Saved Game", use_container_width=True):
                st.session_state.game = load_game(selected)
                st.rerun()
        with load_cols[1]:
            if st.button("Delete Saved Slot", use_container_width=True):
                delete_game(selected)
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if st.button("Start Life", use_container_width=True):
        start_new_life(name, background, life_stage)
        st.rerun()


def render_game_over(game: dict) -> None:
    outcome = game["ending"]
    if outcome == "stable_life":
        st.success("You built a stable life without losing everything important along the way.")
    elif outcome == "housing_loss":
        st.error("Housing instability caught up to you before the rest of your life could recover.")
    elif outcome == "community_builder":
        st.success("You became someone your neighborhood could rely on.")
    elif outcome == "family_legacy":
        st.success("You built a family-centered legacy under pressure.")
    elif outcome == "wealthy_but_isolated":
        st.warning("You found money and image, but not peace.")
    elif outcome in {"creative_breakthrough", "athletic_breakthrough"}:
        st.success("You turned a real gift into a real path.")
    elif outcome == "lost_to_drift":
        st.error("Too many unresolved youth choices turned into a life that drifted off track.")
    else:
        st.error("Your path collapsed under the weight of pressure, risk, and unresolved choices.")

    st.subheader("Final Snapshot")
    p = game["player"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Age", p["age"])
    c2.metric("Cash", f"${p['cash']}")
    c3.metric("Family Bond", p["family_bond"])
    c4.metric("Reputation", p["reputation"])
    if game.get("ending_summary"):
        st.caption(game["ending_summary"])

    st.subheader("How It Played Out")
    for entry in game["log"][:12]:
        st.write(f"- {entry}")

    if st.button("Start New Life", use_container_width=True):
        st.session_state.game = init_game_state()
        st.rerun()


def render_dashboard(game: dict) -> None:
    p = game["player"]
    world = game["world"]
    household = game["household"]
    arc = game["arc"]
    flags = game["story_flags"]

    st.markdown(
        f"""
        <div class="empire-hero">
            <div class="empire-section-label">Live Run</div>
            <h1>{p['name']}</h1>
            <p>{p['life_stage']} | Age {p['age']} | Background: {p['background']} | Choices feel good in the moment. Consequences arrive later.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    summary_left, summary_mid, summary_right = st.columns([1.1, 1.1, 1.1])
    with summary_left:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">State Of Mind</div>', unsafe_allow_html=True)
        mood = st.columns(3)
        mood[0].metric("Stress", p["stress"])
        mood[1].metric("Temptation", p["temptation"])
        mood[2].metric("Hope", p["hope"])
        inner = st.columns(3)
        inner[0].metric("Discipline", p["discipline"])
        inner[1].metric("Morality", p["morality"])
        inner[2].metric("Confidence", p["confidence"])
        st.markdown("</div>", unsafe_allow_html=True)
    with summary_mid:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">Life Foundations</div>', unsafe_allow_html=True)
        life = st.columns(3)
        life[0].metric("Cash", f"${p['cash']}")
        life[1].metric("Debt", f"${p['debt']}")
        life[2].metric("Income", f"${p['income']}")
        life2 = st.columns(3)
        life2[0].metric("Housing", p["housing_stability"])
        life2[1].metric("Family Bond", p["family_bond"])
        life2[2].metric("Trust", p["relationship_trust"])
        st.markdown("</div>", unsafe_allow_html=True)
    with summary_right:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">Public Identity</div>', unsafe_allow_html=True)
        public = st.columns(3)
        public[0].metric("Reputation", p["reputation"])
        public[1].metric("Neighborhood", p["neighborhood_reputation"])
        public[2].metric("Legal Risk", p["legal_risk"])
        public2 = st.columns(3)
        public2[0].metric("Education", p["education"])
        public2[1].metric("School", p["school_record"])
        public2[2].metric("Bills", f"${world['rent_due'] + world['monthly_bills']}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.divider()
    side_left, side_right = st.columns([1.5, 1])
    with side_left:
        stage_copy = arc["childhood"]
        if p["life_stage"] == "Teen":
            stage_copy = arc["teen"]
        elif p["life_stage"] == "Adult":
            stage_copy = arc["adult"]
        st.markdown('<div class="empire-panel">', unsafe_allow_html=True)
        st.subheader("Current Life Arc")
        st.write(stage_copy)
        st.caption(_build_path_summary(game))
        st.write(
            f"Suspensions: {flags['school_suspensions']} | Mentor support: {flags['mentor_support']} | "
            f"Community path: {flags['community_path']} | Rivalry: {flags['rivalry_level']}"
        )
        st.caption(
            f"Recovery steps: {flags['recovery_steps']} | Provider pressure: {flags['provider_pressure']} | "
            f"Creative lane: {flags['creative_talent']} | Athletic lane: {flags['athletic_talent']}"
        )
        st.caption(
            f"Momentum -> Structured: {flags['structured_momentum']} | Risk: {flags['risk_momentum']} | "
            f"Family: {flags['family_momentum']} | Community: {flags['community_momentum']}"
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with side_right:
        st.markdown('<div class="empire-panel">', unsafe_allow_html=True)
        st.subheader("Save Your Run")
        default_slot = f"{p['name'] or 'player'}_age_{p['age']}"
        slot_name = st.text_input("Save slot name", value=default_slot, key="save_slot_name")
        if st.button("Save Game", use_container_width=True):
            saved_slot = save_game(game, slot_name)
            st.success(f"Saved to slot: {saved_slot}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.divider()
    current_event = game.get("current_event")
    if current_event:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">Live Situation</div>', unsafe_allow_html=True)
        st.subheader(current_event["title"])
        st.warning(current_event["body"])
        event_cols = st.columns(len(current_event["options"]))
        for index, option in enumerate(current_event["options"]):
            with event_cols[index]:
                if st.button(option["label"], key=f"event_{option['id']}", use_container_width=True):
                    resolve_event(game, option["id"])
                    check_end_state(game)
                    st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        st.divider()

    choice_left, choice_right = st.columns([1.15, 0.85])
    with choice_left:
        st.markdown('<div class="empire-action-shell">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">Decision Point</div>', unsafe_allow_html=True)
        st.subheader(f"Year {game['turn']}: Choose Your Move")
        st.caption("Each turn should feel like a tradeoff, not just a button click.")
        choices = get_available_choices(game)
        action_cols = st.columns(2)
        for index, choice in enumerate(choices):
            with action_cols[index % 2]:
                help_text = choice.get("description")
                if st.button(choice["label"], use_container_width=True, help=help_text):
                    process_choice(game, choice["id"])
                    clamp_player(p)
                    end_turn_update(game)
                    check_end_state(game)
                    st.rerun()
                if help_text:
                    st.caption(help_text)
        st.markdown("</div>", unsafe_allow_html=True)
    with choice_right:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.markdown('<div class="empire-section-label">Household</div>', unsafe_allow_html=True)
        st.subheader(f"Household in {world['neighborhood']}")
        st.write(f"**Guardian:** {household['guardian']['name']}  \nBond: {household['guardian']['bond']}")
        st.write(f"**Sibling:** {household['sibling']['name']}  \nBond: {household['sibling']['bond']}")
        partner_name = household["partner"]["name"] if household["partner"]["active"] else "No current partner"
        partner_bond = household["partner"]["bond"] if household["partner"]["active"] else "-"
        st.write(f"**Partner:** {partner_name}  \nBond: {partner_bond}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    left, right = st.columns([1, 1])
    with left:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.subheader("Current Pressure")
        st.write(
            """
            - Temptation rises when fast money or status keeps working.
            - Family bond protects you, but neglect erodes support over time.
            - School record and confidence shape youth outcomes before adult life starts.
            - Education and discipline open slower, safer adult outcomes.
            - Debt, housing pressure, and legal risk stack quietly until they don't.
            """
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown('<div class="empire-story-card">', unsafe_allow_html=True)
        st.subheader("Recent Events")
        for entry in game["log"][:8]:
            st.write(f"- {entry}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.divider()
    if st.button("Restart Prototype", use_container_width=True):
        st.session_state.game = init_game_state()
        st.rerun()


def _build_path_summary(game: dict) -> str:
    player = game["player"]
    flags = game["story_flags"]
    lean = []
    if player["education"] >= 65 or player["school_record"] >= 65:
        lean.append("structured")
    if player["street_influence"] >= 35 or player["temptation"] >= 60:
        lean.append("risky")
    if player["family_bond"] >= 60 or player["has_child"]:
        lean.append("family-centered")
    if player["neighborhood_reputation"] >= 30:
        lean.append("community-rooted")
    if flags["creative_talent"] >= 2:
        lean.append("creative")
    if flags["athletic_talent"] >= 2:
        lean.append("athletic")
    if not lean:
        lean.append("unsettled")
    unlocked = []
    if flags["unlocked_school_path"]:
        unlocked.append("school")
    if flags["unlocked_risk_path"]:
        unlocked.append("risk")
    if flags["unlocked_family_path"]:
        unlocked.append("family")
    if flags["unlocked_community_path"]:
        unlocked.append("community")
    unlocked_text = ", ".join(unlocked) if unlocked else "none yet"
    return f"Current path identity: {', '.join(lean)}. Unlocked lanes: {unlocked_text}."


if "game" not in st.session_state:
    st.session_state.game = init_game_state()

game = st.session_state.game

if not game["started"]:
    render_start_screen()
elif game["game_over"]:
    render_game_over(game)
else:
    render_dashboard(game)
