#!/usr/bin/env python
# ruff: noqa: E402
"""Estimate OpenAI token cost for a process_from_flow run from llm_log.jsonl."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

PROCESS_FROM_FLOW_ARTIFACTS_ROOT = Path("artifacts/process_from_flow")
LATEST_RUN_ID_PATH = PROCESS_FROM_FLOW_ARTIFACTS_ROOT / ".latest_run_id"
DEFAULT_REPORT_FILENAME = "llm_cost_report.json"
DEFAULT_INPUT_PRICE_PER_1M = Decimal("0.175")
DEFAULT_OUTPUT_PRICE_PER_1M = Decimal("1.75")
MILLION = Decimal("1000000")
MONEY_QUANT = Decimal("0.00000001")


def _coerce_decimal_price(value: Decimal | float | int | str, *, arg_name: str) -> Decimal:
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = Decimal(str(value))
    elif isinstance(value, str):
        parsed = Decimal(value.strip())
    else:
        raise ValueError(f"Invalid {arg_name}: {value!r}")
    if parsed < 0:
        raise ValueError(f"{arg_name} must be >= 0: {value}")
    return parsed


def _parse_decimal(value: str) -> Decimal:
    try:
        parsed = _coerce_decimal_price(value, arg_name="price")
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid decimal value: {value}") from exc
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", help="Run ID under artifacts/process_from_flow.")
    parser.add_argument("--log-path", type=Path, help="Explicit llm_log.jsonl path.")
    parser.add_argument(
        "--input-price-per-1m",
        type=_parse_decimal,
        default=DEFAULT_INPUT_PRICE_PER_1M,
        help="USD price per 1M input tokens (e.g. 0.175).",
    )
    parser.add_argument(
        "--output-price-per-1m",
        type=_parse_decimal,
        default=DEFAULT_OUTPUT_PRICE_PER_1M,
        help="USD price per 1M output tokens (e.g. 1.75).",
    )
    parser.add_argument("--output", type=Path, help="Output report JSON path.")
    parser.add_argument("--print-json", action="store_true", help="Print final report JSON to stdout.")
    return parser.parse_args()


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _resolve_cli_run_id(args: argparse.Namespace) -> str | None:
    return _resolve_run_id(args.run_id, args.log_path)


def _resolve_run_id(run_id: str | None, log_path: Path | str | None) -> str | None:
    if run_id:
        return str(run_id).strip() or None
    if log_path:
        try:
            return Path(log_path).resolve().parents[1].name
        except Exception:
            return None
    if LATEST_RUN_ID_PATH.exists():
        latest = LATEST_RUN_ID_PATH.read_text(encoding="utf-8").strip()
        return latest or None
    return None


def _resolve_log_path(log_path: Path | str | None, run_id: str | None) -> Path:
    if log_path:
        return Path(log_path)
    if not run_id:
        raise SystemExit("Missing --run-id/--log-path and no latest run marker found.")
    return PROCESS_FROM_FLOW_ARTIFACTS_ROOT / run_id / "cache" / "llm_log.jsonl"


def _resolve_output_path(output_path: Path | str | None, log_path: Path) -> Path:
    if output_path:
        return Path(output_path)
    return log_path.parent / DEFAULT_REPORT_FILENAME


def _load_jsonl(path: Path) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    malformed = 0
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
            continue
        if isinstance(payload, dict):
            rows.append(payload)
        else:
            malformed += 1
    return rows, malformed


def _extract_usage_tokens(record: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    usage = record.get("usage")
    if not isinstance(usage, dict):
        usage = {}
    input_tokens = _coerce_int(usage.get("input_tokens"))
    output_tokens = _coerce_int(usage.get("output_tokens"))
    total_tokens = _coerce_int(usage.get("total_tokens"))

    # Backward/forward compatibility for flat fields.
    if input_tokens is None:
        input_tokens = _coerce_int(record.get("input_tokens"))
    if output_tokens is None:
        output_tokens = _coerce_int(record.get("output_tokens"))
    if total_tokens is None:
        total_tokens = _coerce_int(record.get("total_tokens"))
    if total_tokens is None and input_tokens is not None and output_tokens is not None:
        total_tokens = input_tokens + output_tokens
    return input_tokens, output_tokens, total_tokens


def _cost_for_tokens(tokens: int, price_per_1m: Decimal) -> Decimal:
    if tokens <= 0:
        return Decimal("0")
    return (Decimal(tokens) / MILLION) * price_per_1m


def _money(value: Decimal) -> float:
    return float(value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP))


def generate_cost_report(
    *,
    run_id: str | None = None,
    log_path: Path | str | None = None,
    input_price_per_1m: Decimal | float | int | str = DEFAULT_INPUT_PRICE_PER_1M,
    output_price_per_1m: Decimal | float | int | str = DEFAULT_OUTPUT_PRICE_PER_1M,
    output_path: Path | str | None = None,
) -> tuple[dict[str, Any], Path]:
    resolved_run_id = _resolve_run_id(run_id, log_path)
    resolved_log_path = _resolve_log_path(log_path, resolved_run_id)
    if not resolved_log_path.exists():
        raise FileNotFoundError(f"LLM log not found: {resolved_log_path}")
    resolved_output_path = _resolve_output_path(output_path, resolved_log_path)

    input_price = _coerce_decimal_price(input_price_per_1m, arg_name="input_price_per_1m")
    output_price = _coerce_decimal_price(output_price_per_1m, arg_name="output_price_per_1m")

    rows, malformed_lines = _load_jsonl(resolved_log_path)
    stage_totals: dict[str, dict[str, int]] = {}

    records_total = len(rows)
    records_ok = 0
    records_error = 0
    api_calls = 0
    cache_hits = 0
    missing_usage_calls = 0
    input_tokens_total = 0
    output_tokens_total = 0
    total_tokens_total = 0

    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        stage = str(row.get("stage") or "unknown").strip() or "unknown"
        cache_hit = bool(row.get("cache_hit"))
        if status == "ok":
            records_ok += 1
        else:
            records_error += 1
        if status != "ok":
            continue
        if cache_hit:
            cache_hits += 1
            continue

        api_calls += 1
        input_tokens, output_tokens, total_tokens = _extract_usage_tokens(row)
        if input_tokens is None and output_tokens is None:
            missing_usage_calls += 1
            continue

        if stage not in stage_totals:
            stage_totals[stage] = {
                "api_calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            }
        stage_totals[stage]["api_calls"] += 1

        if input_tokens is not None:
            input_tokens_total += input_tokens
            stage_totals[stage]["input_tokens"] += input_tokens
        if output_tokens is not None:
            output_tokens_total += output_tokens
            stage_totals[stage]["output_tokens"] += output_tokens
        if total_tokens is not None:
            total_tokens_total += total_tokens
            stage_totals[stage]["total_tokens"] += total_tokens

    input_cost = _cost_for_tokens(input_tokens_total, input_price)
    output_cost = _cost_for_tokens(output_tokens_total, output_price)
    total_cost = input_cost + output_cost

    stage_rows: list[dict[str, Any]] = []
    for stage, metrics in stage_totals.items():
        stage_input = metrics["input_tokens"]
        stage_output = metrics["output_tokens"]
        stage_input_cost = _cost_for_tokens(stage_input, input_price)
        stage_output_cost = _cost_for_tokens(stage_output, output_price)
        stage_total_cost = stage_input_cost + stage_output_cost
        stage_rows.append(
            {
                "stage": stage,
                "api_calls": metrics["api_calls"],
                "input_tokens": stage_input,
                "output_tokens": stage_output,
                "total_tokens": metrics["total_tokens"],
                "input_cost_usd": _money(stage_input_cost),
                "output_cost_usd": _money(stage_output_cost),
                "total_cost_usd": _money(stage_total_cost),
            }
        )
    stage_rows.sort(key=lambda item: float(item.get("total_cost_usd") or 0.0), reverse=True)

    notes: list[str] = []
    if malformed_lines > 0:
        notes.append(f"Ignored malformed JSONL lines: {malformed_lines}.")
    if missing_usage_calls > 0:
        notes.append(
            "Some non-cache successful calls had no token usage fields; their cost is excluded. "
            "Use updated scripts for accurate accounting on new runs."
        )

    report: dict[str, Any] = {
        "run_id": resolved_run_id,
        "log_path": str(resolved_log_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pricing": {
            "input_price_usd_per_1m_tokens": _money(input_price),
            "output_price_usd_per_1m_tokens": _money(output_price),
        },
        "totals": {
            "records_total": records_total,
            "records_ok": records_ok,
            "records_error": records_error,
            "api_calls": api_calls,
            "cache_hits": cache_hits,
            "missing_usage_calls": missing_usage_calls,
            "input_tokens": input_tokens_total,
            "output_tokens": output_tokens_total,
            "total_tokens": total_tokens_total,
            "input_cost_usd": _money(input_cost),
            "output_cost_usd": _money(output_cost),
            "total_cost_usd": _money(total_cost),
        },
        "by_stage": stage_rows,
        "notes": notes,
    }

    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report, resolved_output_path


def main() -> None:
    args = parse_args()
    run_id = _resolve_cli_run_id(args)
    try:
        report, output_path = generate_cost_report(
            run_id=run_id,
            log_path=args.log_path,
            input_price_per_1m=args.input_price_per_1m,
            output_price_per_1m=args.output_price_per_1m,
            output_path=args.output,
        )
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc
    totals = report.get("totals") if isinstance(report.get("totals"), dict) else {}
    api_calls = int(totals.get("api_calls") or 0)
    cache_hits = int(totals.get("cache_hits") or 0)
    input_tokens_total = int(totals.get("input_tokens") or 0)
    output_tokens_total = int(totals.get("output_tokens") or 0)
    total_cost_usd = float(totals.get("total_cost_usd") or 0.0)

    print(
        (
            f"run_id={run_id or 'unknown'} api_calls={api_calls} cache_hits={cache_hits} "
            f"input_tokens={input_tokens_total} output_tokens={output_tokens_total} "
            f"total_cost_usd={total_cost_usd:.8f}"
        ),
        file=sys.stderr,
    )
    print(f"report={output_path}", file=sys.stderr)
    if args.print_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":  # pragma: no cover
    main()
