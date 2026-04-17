import json
from copy import deepcopy
from pathlib import Path


SAVE_DIR = Path(__file__).resolve().parent.parent / "saves"


def ensure_save_dir() -> Path:
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    return SAVE_DIR


def list_save_slots() -> list[str]:
    save_dir = ensure_save_dir()
    return sorted(path.stem for path in save_dir.glob("*.json"))


def save_game(game: dict, slot_name: str) -> str:
    save_dir = ensure_save_dir()
    sanitized = _sanitize_slot_name(slot_name)
    payload = deepcopy(game)
    payload["save_meta"] = {
        "slot": sanitized,
        "player_name": payload["player"]["name"] or "Player",
        "age": payload["player"]["age"],
        "stage": payload["player"]["life_stage"],
        "ending": payload["ending"],
    }
    (save_dir / f"{sanitized}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return sanitized


def load_game(slot_name: str) -> dict:
    path = ensure_save_dir() / f"{_sanitize_slot_name(slot_name)}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def delete_game(slot_name: str) -> None:
    path = ensure_save_dir() / f"{_sanitize_slot_name(slot_name)}.json"
    if path.exists():
        path.unlink()


def _sanitize_slot_name(slot_name: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in slot_name.strip())
    collapsed = "_".join(part for part in cleaned.split("_") if part)
    return collapsed or "empire_life_save"
