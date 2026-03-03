#!/usr/bin/env python3
"""List direct children in the Tiangong product flow category hierarchy.

Prints rows as "<code>\\t<description>" so callers can parse stdout easily.
With no argument, lists root-level categories.
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path


def _usage() -> str:
    return (
        "Usage: list_product_flow_category_children.py [category_code]\n"
        "  Omit category_code to list root-level categories.\n"
        "  Prints rows as: <code>\\t<description>\n"
    )


def _load_schema_path() -> Path:
    env_override = (os.environ.get("PAB_PRODUCT_CATEGORY_SCHEMA") or "").strip()
    env_path = Path(env_override).expanduser() if env_override else None
    if env_path is not None:
        return env_path
    try:
        import tidas_tools  # type: ignore
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency 'tidas_tools'. Run scripts/setup-process-automated-builder.sh first "
            "or set PAB_PRODUCT_CATEGORY_SCHEMA."
        ) from exc
    return Path(tidas_tools.__file__).resolve().parent / "tidas" / "schemas" / "tidas_flows_product_category.json"


def _load_rows() -> list[tuple[int, str, str]]:
    schema_path = _load_schema_path()
    if not schema_path.exists():
        raise SystemExit(f"Schema file not found: {schema_path}")
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    one_of = data.get("oneOf")
    if not isinstance(one_of, list):
        raise SystemExit(f"Unexpected schema format (missing oneOf list): {schema_path}")

    rows: list[tuple[int, str, str]] = []
    for item in one_of:
        if not isinstance(item, dict):
            continue
        props = item.get("properties")
        if not isinstance(props, dict):
            continue
        raw_level = ((props.get("@level") or {}) if isinstance(props.get("@level"), dict) else {}).get("const")
        raw_code = ((props.get("@classId") or {}) if isinstance(props.get("@classId"), dict) else {}).get("const")
        raw_text = ((props.get("#text") or {}) if isinstance(props.get("#text"), dict) else {}).get("const")
        if raw_level is None or raw_code is None:
            continue
        try:
            level = int(raw_level)
        except (TypeError, ValueError):
            continue
        code = str(raw_code).strip()
        text = "" if raw_text is None else str(raw_text).strip()
        if code:
            rows.append((level, code, text))
    return rows


def _build_children(rows: list[tuple[int, str, str]]) -> dict[str | None, list[tuple[str, str]]]:
    children: dict[str | None, list[tuple[str, str]]] = defaultdict(list)
    stack: list[tuple[int, str]] = []
    for level, code, text in rows:
        while stack and stack[-1][0] >= level:
            stack.pop()
        parent_code = stack[-1][1] if stack else None
        children[parent_code].append((code, text))
        stack.append((level, code))
    return children


def main(argv: list[str]) -> int:
    if any(arg in {"-h", "--help"} for arg in argv[1:]):
        sys.stdout.write(_usage())
        return 0
    if len(argv) > 2:
        sys.stderr.write(_usage())
        return 2

    target_code = argv[1].strip() if len(argv) == 2 else None
    if target_code == "":
        target_code = None

    rows = _load_rows()
    children_map = _build_children(rows)
    for code, text in children_map.get(target_code, []):
        sys.stdout.write(f"{code}\t{text}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
