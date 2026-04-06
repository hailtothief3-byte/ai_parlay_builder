from __future__ import annotations

import importlib
from pathlib import Path
import sys


ENV_PATH = Path(".env")


def _read_env_lines() -> list[str]:
    if not ENV_PATH.exists():
        return []
    return ENV_PATH.read_text(encoding="utf-8").splitlines()


def upsert_env_values(updates: dict[str, str]) -> None:
    lines = _read_env_lines()
    remaining = dict(updates)
    new_lines: list[str] = []

    for line in lines:
        if "=" not in line or line.strip().startswith("#"):
            new_lines.append(line)
            continue

        key, _, _ = line.partition("=")
        stripped_key = key.strip()
        if stripped_key in remaining:
            new_lines.append(f"{stripped_key}={remaining.pop(stripped_key)}")
        else:
            new_lines.append(line)

    for key, value in remaining.items():
        new_lines.append(f"{key}={value}")

    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def reload_runtime_modules() -> None:
    module_names = [
        "config",
        "services.usage_guard",
        "services.sync_policy",
        "ingestion.sportsgameodds_api",
        "ingestion.providers.sportsgameodds_provider",
    ]

    for module_name in module_names:
        module = sys.modules.get(module_name)
        if module is not None:
            importlib.reload(module)
