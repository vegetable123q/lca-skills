"""Shared utilities for staged Tiangong LCA workflow scripts."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from tiangong_lca_spec.state_lock import hold_state_file_lock, is_process_state_path
except Exception:  # pragma: no cover
    hold_state_file_lock = None

    def is_process_state_path(path: Path) -> bool:
        return False


class OpenAIResponsesLLM:
    """Minimal wrapper around the OpenAI Responses API with lightweight disk caching."""

    def __init__(
        self,
        api_key: str,
        model: str,
        timeout: int = 600,
        cache_dir: Path | None = Path("artifacts/cache/openai"),
        use_cache: bool = True,
        base_url: str | None = None,
        run_id: str | None = None,
        module: str = "unknown",
        stage: str = "unknown",
        trace_path: Path | None = None,
    ) -> None:
        try:
            from openai import APIConnectionError, APIStatusError, OpenAI
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise SystemExit("Missing dependency 'openai'. Run scripts/setup-process-automated-builder.sh first.") from exc

        self._api_connection_error_cls = APIConnectionError
        self._api_status_error_cls = APIStatusError
        client_kwargs: dict[str, Any] = {"api_key": api_key, "timeout": timeout}
        if base_url:
            client_kwargs["base_url"] = base_url
        self._client = OpenAI(**client_kwargs)
        self._model = model
        self._base_url = base_url
        self._cache_dir = Path(cache_dir) if use_cache and cache_dir else None
        if self._cache_dir:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
        env_run_id = (os.getenv("TIANGONG_PFF_RUN_ID") or "").strip()
        self._run_id = (run_id or env_run_id or "unknown").strip() or "unknown"
        self._module = module
        self._stage = stage
        self._trace_path = self._resolve_trace_path(trace_path)

    def _resolve_trace_path(self, trace_path: Path | None) -> Path | None:
        if trace_path is not None:
            trace_path.parent.mkdir(parents=True, exist_ok=True)
            return trace_path
        env_trace = (os.getenv("TIANGONG_PFF_LLM_TRACE_PATH") or "").strip()
        if env_trace:
            target = Path(env_trace)
            target.parent.mkdir(parents=True, exist_ok=True)
            return target
        if self._run_id and self._run_id != "unknown":
            target = Path("artifacts") / "process_from_flow" / self._run_id / "cache" / "llm_log.jsonl"
            target.parent.mkdir(parents=True, exist_ok=True)
            return target
        return None

    def _append_trace(
        self,
        *,
        digest: str,
        cache_hit: bool,
        status: str,
        latency_ms: float,
        usage: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if not self._trace_path:
            return
        record: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": self._run_id,
            "stage": self._stage,
            "module": self._module,
            "prompt_hash": digest,
            "latency_ms": round(latency_ms, 2),
            "cache_hit": cache_hit,
            "status": status,
            "model": self._model,
        }
        if isinstance(usage, dict) and usage:
            record["usage"] = usage
            input_tokens = usage.get("input_tokens")
            output_tokens = usage.get("output_tokens")
            total_tokens = usage.get("total_tokens")
            if isinstance(input_tokens, int):
                record["input_tokens"] = input_tokens
            if isinstance(output_tokens, int):
                record["output_tokens"] = output_tokens
            if isinstance(total_tokens, int):
                record["total_tokens"] = total_tokens
        if error:
            record["error"] = error[:500]
        try:
            with self._trace_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            return

    def invoke(self, input_data: dict[str, Any]) -> str:
        prompt = input_data.get("prompt") or ""
        context = input_data.get("context")
        response_format = input_data.get("response_format")
        text_config = input_data.get("text")
        if isinstance(context, (dict, list)):
            user_content = json.dumps(context, ensure_ascii=False)
        else:
            user_content = str(context) if context is not None else ""
        payload = [
            {"role": "system", "content": [{"type": "input_text", "text": str(prompt)}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_content}]},
        ]

        text_options = dict(text_config or {})
        if response_format:
            text_options["format"] = response_format

        cache_path = self._cache_lookup(payload, text_options)
        digest = cache_path.stem if cache_path else hashlib.sha256(
            json.dumps({"payload": payload, "text_options": text_options}, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        started = time.monotonic()
        if cache_path and cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            self._append_trace(
                digest=digest,
                cache_hit=True,
                status="ok",
                latency_ms=(time.monotonic() - started) * 1000,
            )
            return cached["output"]

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                kwargs: dict[str, Any] = {"model": self._model, "input": payload}
                if text_options:
                    kwargs["text"] = text_options
                response = self._client.responses.create(**kwargs)
                output = self._extract_output(response)
                usage = self._extract_usage(response)
                if cache_path:
                    self._cache_store(cache_path, {"output": output, "usage": usage})
                self._append_trace(
                    digest=digest,
                    cache_hit=False,
                    status="ok",
                    latency_ms=(time.monotonic() - started) * 1000,
                    usage=usage,
                )
                return output
            except (self._api_connection_error_cls, self._api_status_error_cls) as exc:
                last_error = exc
                if attempt == 2:
                    self._append_trace(
                        digest=digest,
                        cache_hit=False,
                        status="error",
                        latency_ms=(time.monotonic() - started) * 1000,
                        error=str(exc),
                    )
                    raise
                time.sleep(5 * (attempt + 1))
        if last_error:
            raise last_error
        raise RuntimeError("OpenAI invocation failed without response")

    def _cache_lookup(self, payload: list[dict[str, Any]], text_options: dict[str, Any]) -> Path | None:
        if not self._cache_dir:
            return None
        cache_material = {
            "model": self._model,
            "base_url": self._base_url,
            "payload": payload,
            "text_options": text_options,
        }
        digest = hashlib.sha256(json.dumps(cache_material, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        return self._cache_dir / f"{digest}.json"

    def _cache_store(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=path.parent, encoding="utf-8", delete=False) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
            temp_name = tmp.name
        os.replace(temp_name, path)

    @staticmethod
    def _extract_output(response: Any) -> str:
        if getattr(response, "output_text", None):
            return response.output_text
        parts: list[str] = []
        for item in getattr(response, "output", []) or []:
            if item.get("type") == "message":
                for content in item["content"]:
                    if content.get("type") == "output_text":
                        parts.append(content.get("text", ""))
        return "\n".join(parts)

    @staticmethod
    def _usage_get(obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    @staticmethod
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

    @classmethod
    def _extract_usage(cls, response: Any) -> dict[str, Any]:
        usage_obj = getattr(response, "usage", None)
        if usage_obj is None and isinstance(response, dict):
            usage_obj = response.get("usage")
        if usage_obj is None:
            return {}

        usage: dict[str, Any] = {}
        input_tokens = cls._coerce_int(cls._usage_get(usage_obj, "input_tokens"))
        output_tokens = cls._coerce_int(cls._usage_get(usage_obj, "output_tokens"))
        total_tokens = cls._coerce_int(cls._usage_get(usage_obj, "total_tokens"))

        input_details = cls._usage_get(usage_obj, "input_tokens_details")
        cached_tokens = cls._coerce_int(cls._usage_get(input_details, "cached_tokens"))

        output_details = cls._usage_get(usage_obj, "output_tokens_details")
        reasoning_tokens = cls._coerce_int(cls._usage_get(output_details, "reasoning_tokens"))

        if input_tokens is not None:
            usage["input_tokens"] = input_tokens
        if output_tokens is not None:
            usage["output_tokens"] = output_tokens
        if total_tokens is not None:
            usage["total_tokens"] = total_tokens
        elif input_tokens is not None and output_tokens is not None:
            usage["total_tokens"] = input_tokens + output_tokens
        if cached_tokens is not None:
            usage["cached_input_tokens"] = cached_tokens
        if reasoning_tokens is not None:
            usage["reasoning_tokens"] = reasoning_tokens
        return usage


def load_openai_from_env() -> tuple[str, str, str | None]:
    """Load OpenAI API credentials from environment variables only."""
    api_key = (os.getenv("OPENAI_API_KEY") or os.getenv("LCA_OPENAI_API_KEY") or "").strip()
    if api_key.lower().startswith("bearer "):
        api_key = api_key[7:].strip()
    if not api_key:
        raise SystemExit("OpenAI API key missing. Set OPENAI_API_KEY (or LCA_OPENAI_API_KEY).")

    model = (os.getenv("OPENAI_MODEL") or os.getenv("LCA_OPENAI_MODEL") or "gpt-5").strip() or "gpt-5"
    base_url = (os.getenv("OPENAI_BASE_URL") or os.getenv("LCA_OPENAI_BASE_URL") or "").strip() or None
    return api_key, model, base_url


def load_paper(path: Path) -> str:
    """Load the paper content, accepting raw markdown or JSON fragments."""
    raw = path.read_text(encoding="utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if isinstance(parsed, dict) and "result" in parsed:
        fragments = [item.get("text", "") for item in parsed["result"] if isinstance(item, dict) and item.get("text")]
        return json.dumps(fragments, ensure_ascii=False)
    return raw


def dump_json(data: Any, path: Path, *, lock_reason: str | None = None) -> None:
    """Write JSON to disk with UTF-8 encoding, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    if hold_state_file_lock is not None and is_process_state_path(path):
        reason = lock_reason or "scripts.dump_json_state_write"
        with hold_state_file_lock(path, reason=reason):
            path.write_text(payload, encoding="utf-8")
        return
    path.write_text(payload, encoding="utf-8")


ARTIFACTS_ROOT = Path("artifacts")
LATEST_RUN_ID_PATH = ARTIFACTS_ROOT / ".latest_run_id"
RUN_CACHE_DIRNAME = "cache"
RUN_EXPORT_DIRNAME = "exports"


def generate_run_id() -> str:
    """Return a UTC timestamp-based identifier, e.g., 20251030T053000Z."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_run_root(run_id: str) -> Path:
    """Create (if needed) and return the root directory for a run."""
    run_root = ARTIFACTS_ROOT / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    return run_root


def ensure_run_cache_dir(run_id: str) -> Path:
    """Create (if needed) and return the cache directory for a run."""
    run_root = ensure_run_root(run_id)
    cache_dir = run_root / RUN_CACHE_DIRNAME
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def ensure_run_exports_dir(run_id: str, *, clean: bool = False) -> Path:
    """Create (if needed) and return the exports directory for a run."""
    run_root = ensure_run_root(run_id)
    export_root = run_root / RUN_EXPORT_DIRNAME
    if clean and export_root.exists():
        shutil.rmtree(export_root)
    for name in ("processes", "flows", "sources"):
        (export_root / name).mkdir(parents=True, exist_ok=True)
    return export_root


def _latest_run_id_path(pipeline: str | None = None) -> Path:
    """Return the filesystem marker for the given pipeline."""
    if pipeline == "jsonld":
        return ARTIFACTS_ROOT / ".latest_jsonld_run_id"
    return LATEST_RUN_ID_PATH


def resolve_run_id(run_id: str | None, pipeline: str | None = None) -> str:
    """Return the provided run ID or fall back to the most recent run."""
    if run_id:
        return run_id
    latest = load_latest_run_id(pipeline=pipeline)
    if latest:
        return latest
    raise SystemExit("Run ID not provided and no previous run metadata found. " "Run stage1_preprocess first or supply --run-id explicitly.")


def load_latest_run_id(path: Path | None = None, *, pipeline: str | None = None) -> str | None:
    """Load the latest run identifier recorded on disk, if any."""
    target = path or _latest_run_id_path(pipeline)
    if not target.exists():
        return None
    run_id = target.read_text(encoding="utf-8").strip()
    return run_id or None


def save_latest_run_id(run_id: str, path: Path | None = None, *, pipeline: str | None = None) -> None:
    """Persist the most recent run identifier for subsequent stages."""
    target = path or _latest_run_id_path(pipeline)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(run_id, encoding="utf-8")


def run_cache_path(run_id: str, relative: str | Path) -> Path:
    """Return a path under the run-specific cache directory."""
    cache_dir = ensure_run_cache_dir(run_id)
    return cache_dir / Path(relative)
