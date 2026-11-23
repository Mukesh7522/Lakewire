"""Data contract: classify, validate and normalise raw recentchange events."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).resolve().parent / "contracts" / "recentchange.schema.json"
WANTED_TYPES = frozenset({"edit", "new"})


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    return Draft202012Validator(json.loads(SCHEMA_PATH.read_text()))


def validation_errors(raw: dict[str, Any]) -> list[str]:
    return [
        f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in sorted(_validator().iter_errors(raw), key=lambda e: list(e.absolute_path))
    ]


def normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """Flatten a validated raw event into the row shape stored in Bronze."""
    length = raw.get("length") or {}
    revision = raw.get("revision") or {}
    return {
        "event_id": raw["meta"]["id"],
        "event_ts": raw["meta"]["dt"],
        "wiki": raw["wiki"],
        "event_type": raw["type"],
        "title": raw["title"],
        "namespace": raw.get("namespace"),
        "user": raw["user"],
        "is_bot": bool(raw.get("bot", False)),
        "is_minor": bool(raw.get("minor", False)),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "rev_old": revision.get("old"),
        "rev_new": revision.get("new"),
    }


def classify(raw: Any, wikis: tuple[str, ...]) -> tuple[str, Any]:
    """Return ("skip", None), ("dlq", [errors]) or ("ok", normalised_event)."""
    if not isinstance(raw, dict):
        return "dlq", ["<root>: event is not a JSON object"]
    meta = raw.get("meta")
    if isinstance(meta, dict) and meta.get("domain") == "canary":
        # Wikimedia emits synthetic canary events every few minutes to test its own pipelines.
        # They carry only $schema and meta, so they are not bad data and must not reach the DLQ.
        return "skip", None
    etype, wiki = raw.get("type"), raw.get("wiki")
    if isinstance(etype, str) and isinstance(wiki, str):
        if etype not in WANTED_TYPES or wiki not in wikis:
            return "skip", None
    # Missing or mistyped type/wiki falls through to validation and ends in the DLQ.
    errors = validation_errors(raw)
    if errors:
        return "dlq", errors
    return "ok", normalize(raw)
