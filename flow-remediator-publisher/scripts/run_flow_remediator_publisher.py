#!/usr/bin/env python3
"""Initial flow remediation + append-only publish pipeline.

This script is intentionally split into subcommands so it can be used as:
- a one-shot pipeline (`pipeline`)
- or a staged workflow (`fetch`, `review`, `llm-remediate`, `validate-schema`, `validate`, `publish`)

Design goals for the initial version:
- No direct DB access; all remote access goes through MCP CRUD.
- Reuse `process-automated-builder` services for CRUD and product flow regeneration.
- Keep review/fix logic minimal but structured so it can later hand off to `lifecycleinventory-review --profile flow`.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, MutableMapping


UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

DATABASE_TOOL_NAME = "Database_CRUD_Tool"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: Any) -> None:
    _ensure_parent(path)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    _ensure_parent(path)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _sha256_json(data: Any) -> str:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        text = value.get("#text")
        if isinstance(text, str):
            return text.strip()
    return str(value).strip()


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _deep_get(obj: Mapping[str, Any] | None, path: tuple[str, ...]) -> Any:
    current: Any = obj
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _walk_strings(node: Any) -> Iterator[str]:
    if isinstance(node, str):
        text = node.strip()
        if text:
            yield text
        return
    if isinstance(node, Mapping):
        text = node.get("#text")
        if isinstance(text, str):
            stripped = text.strip()
            if stripped:
                yield stripped
        for value in node.values():
            yield from _walk_strings(value)
        return
    if isinstance(node, list):
        for item in node:
            yield from _walk_strings(item)


def _flow_root(doc: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(doc.get("flowDataSet"), Mapping):
        return copy.deepcopy(doc["flowDataSet"])  # type: ignore[index]
    return copy.deepcopy(dict(doc))


def _flow_wrapper(doc: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(doc.get("flowDataSet"), Mapping):
        return copy.deepcopy(dict(doc))
    return {"flowDataSet": copy.deepcopy(dict(doc))}


def _flow_uuid(flow_ds: Mapping[str, Any]) -> str:
    return _coerce_text(
        _deep_get(flow_ds, ("flowInformation", "dataSetInformation", "common:UUID"))
    )


def _flow_version(flow_ds: Mapping[str, Any]) -> str:
    return _coerce_text(
        _deep_get(
            flow_ds,
            ("administrativeInformation", "publicationAndOwnership", "common:dataSetVersion"),
        )
    )


def _set_flow_version(flow_ds: MutableMapping[str, Any], version: str) -> None:
    flow_ds.setdefault("administrativeInformation", {})
    admin = flow_ds["administrativeInformation"]
    if not isinstance(admin, MutableMapping):
        flow_ds["administrativeInformation"] = {}
        admin = flow_ds["administrativeInformation"]
    admin.setdefault("publicationAndOwnership", {})
    pub = admin["publicationAndOwnership"]
    if not isinstance(pub, MutableMapping):
        admin["publicationAndOwnership"] = {}
        pub = admin["publicationAndOwnership"]
    pub["common:dataSetVersion"] = version


def _flow_type(flow_ds: Mapping[str, Any]) -> str:
    return _coerce_text(
        _deep_get(flow_ds, ("modellingAndValidation", "LCIMethod", "typeOfDataSet"))
    )


def _name_node(flow_ds: Mapping[str, Any]) -> Any:
    return _deep_get(flow_ds, ("flowInformation", "dataSetInformation", "name"))


def _name_texts(flow_ds: Mapping[str, Any]) -> list[str]:
    return list(_walk_strings(_name_node(flow_ds)))


def _classification_classes(flow_ds: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = _deep_get(
        flow_ds,
        (
            "flowInformation",
            "dataSetInformation",
            "classificationInformation",
            "common:classification",
            "common:class",
        ),
    )
    out: list[dict[str, Any]] = []
    for item in _as_list(raw):
        if isinstance(item, Mapping):
            out.append(dict(item))
    return out


def _classification_leaf(flow_ds: Mapping[str, Any]) -> dict[str, str]:
    classes = _classification_classes(flow_ds)
    if not classes:
        return {"class_id": "", "text": "", "key": ""}
    leaf = classes[-1]
    class_id = _coerce_text(leaf.get("@classId"))
    text = _coerce_text(leaf.get("#text"))
    key = f"{class_id}|{text}".strip("|")
    return {"class_id": class_id, "text": text, "key": key}


def _lang_text(items: Any, lang: str) -> str:
    for item in _as_list(items):
        if isinstance(item, Mapping):
            if _coerce_text(item.get("@xml:lang")).lower() == lang.lower():
                text = _coerce_text(item.get("#text"))
                if text:
                    return text
    # Fallback to first textual entry
    for item in _as_list(items):
        text = _coerce_text(item)
        if text:
            return text
    return ""


def _name_primary(flow_ds: Mapping[str, Any], lang: str = "en") -> str:
    name = _name_node(flow_ds)
    if isinstance(name, Mapping):
        return _lang_text(name.get("baseName"), lang)
    return ""


def _normalize_name_token(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _name_fingerprint(flow_ds: Mapping[str, Any]) -> str:
    name = _name_node(flow_ds)
    if not isinstance(name, Mapping):
        return ""
    parts = []
    for key in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes"):
        pieces = [_coerce_text(item.get("#text") if isinstance(item, Mapping) else item) for item in _as_list(name.get(key))]
        pieces = [p for p in pieces if p]
        if pieces:
            parts.append(" | ".join(pieces))
    if not parts:
        parts = _name_texts(flow_ds)
    return _normalize_name_token(" || ".join(parts))


def _flow_properties(flow_ds: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = _deep_get(flow_ds, ("flowProperties", "flowProperty"))
    out: list[dict[str, Any]] = []
    for item in _as_list(raw):
        if isinstance(item, Mapping):
            out.append(dict(item))
    return out


def _pick_reference_flow_property(flow_ds: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str]:
    props = _flow_properties(flow_ds)
    for prop in props:
        internal_id = _coerce_text(prop.get("@dataSetInternalID"))
        if internal_id == "0":
            return prop, internal_id
    if props:
        internal_id = _coerce_text(props[0].get("@dataSetInternalID"))
        return props[0], internal_id
    return None, ""


def _quant_ref_internal_id(flow_ds: Mapping[str, Any]) -> str:
    return _coerce_text(
        _deep_get(flow_ds, ("flowInformation", "quantitativeReference", "referenceToReferenceFlowProperty"))
    )


def _set_quant_ref_internal_id(flow_ds: MutableMapping[str, Any], value: str) -> None:
    flow_ds.setdefault("flowInformation", {})
    info = flow_ds["flowInformation"]
    if not isinstance(info, MutableMapping):
        flow_ds["flowInformation"] = {}
        info = flow_ds["flowInformation"]
    info.setdefault("quantitativeReference", {})
    qref = info["quantitativeReference"]
    if not isinstance(qref, MutableMapping):
        info["quantitativeReference"] = {}
        qref = info["quantitativeReference"]
    qref["referenceToReferenceFlowProperty"] = value


def _ref_uuid_from_reference_node(node: Any) -> str:
    if isinstance(node, Mapping):
        for key in ("@refObjectId", "@uri"):
            raw = _coerce_text(node.get(key))
            if not raw:
                continue
            match = UUID_RE.search(raw)
            if match:
                return match.group(0).lower()
    raw_text = _coerce_text(node)
    match = UUID_RE.search(raw_text)
    return match.group(0).lower() if match else ""


def _ref_version_from_reference_node(node: Any) -> str:
    if isinstance(node, Mapping):
        return _coerce_text(node.get("@version"))
    return ""


def _flow_property_ref(prop: Mapping[str, Any]) -> dict[str, str]:
    ref = prop.get("referenceToFlowPropertyDataSet")
    return {
        "uuid": _ref_uuid_from_reference_node(ref),
        "version": _ref_version_from_reference_node(ref),
        "internal_id": _coerce_text(prop.get("@dataSetInternalID")),
        "short_name": _lang_text(
            (ref.get("common:shortDescription") if isinstance(ref, Mapping) else None),
            "en",
        ),
    }


def _extract_flowproperty_unitgroup_ref(flowprop_root: Mapping[str, Any]) -> str:
    candidates = [
        ("flowPropertyDataSet", "unitGroup", "referenceToUnitGroup"),
        (
            "flowPropertyDataSet",
            "flowPropertiesInformation",
            "quantitativeReference",
            "referenceToReferenceUnitGroup",
        ),
    ]
    # Allow caller to pass raw root or wrapped doc.
    root = flowprop_root
    if "flowPropertyDataSet" in flowprop_root and isinstance(flowprop_root["flowPropertyDataSet"], Mapping):
        root = flowprop_root
    else:
        root = {"flowPropertyDataSet": flowprop_root}
    for path in candidates:
        node = _deep_get(root, path)
        ref_uuid = _ref_uuid_from_reference_node(node)
        if ref_uuid:
            return ref_uuid
    return ""


def _extract_reference_unit_name(unitgroup_root: Mapping[str, Any]) -> str:
    root = unitgroup_root
    if "unitGroupDataSet" in unitgroup_root and isinstance(unitgroup_root["unitGroupDataSet"], Mapping):
        root = unitgroup_root
    else:
        root = {"unitGroupDataSet": unitgroup_root}
    units = _deep_get(root, ("unitGroupDataSet", "units", "unit"))
    unit_items = [u for u in _as_list(units) if isinstance(u, Mapping)]
    # Prefer marked reference unit.
    for unit in unit_items:
        ref_flag = unit.get("referenceUnit")
        ref_text = str(ref_flag).strip().lower() if ref_flag is not None else ""
        if ref_text == "true":
            name = _coerce_text(unit.get("name"))
            if name:
                return name
    for unit in unit_items:
        name = _coerce_text(unit.get("name"))
        if name:
            return name
    return ""


def _is_elementary_flow(flow_type: str) -> bool:
    return flow_type.strip().lower() == "elementary flow"


def _bump_ilcd_version(version: str) -> str:
    text = (version or "").strip()
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", text)
    if not match:
        return "01.01.001"
    a, b, c = match.groups()
    c_num = int(c) + 1
    return f"{int(a):0{len(a)}d}.{int(b):0{len(b)}d}.{c_num:0{len(c)}d}"


def _flow_wrapper_without_version(doc: Mapping[str, Any]) -> dict[str, Any]:
    wrapper = _flow_wrapper(doc)
    flow_ds = wrapper.get("flowDataSet")
    if not isinstance(flow_ds, MutableMapping):
        return wrapper
    admin = flow_ds.get("administrativeInformation")
    if not isinstance(admin, MutableMapping):
        return wrapper
    pub = admin.get("publicationAndOwnership")
    if not isinstance(pub, MutableMapping):
        return wrapper
    pub.pop("common:dataSetVersion", None)
    return wrapper


def _sha256_flow_without_version(doc: Mapping[str, Any]) -> str:
    return _sha256_json(_flow_wrapper_without_version(doc))


def _is_version_conflict_error(message: str) -> bool:
    text = (message or "").strip().lower()
    if not text:
        return False
    if "version" not in text and "data_set_version" not in text and "dataset_version" not in text:
        return False
    conflict_tokens = ("duplicate", "already exists", "unique", "conflict", "violates")
    return any(token in text for token in conflict_tokens)


def _json_pointer_set(root: MutableMapping[str, Any], pointer: str, value: Any) -> None:
    if not pointer.startswith("/"):
        raise ValueError(f"Unsupported JSON pointer: {pointer}")
    parts = [p.replace("~1", "/").replace("~0", "~") for p in pointer.lstrip("/").split("/")]
    current: Any = root
    for key in parts[:-1]:
        if isinstance(current, MutableMapping):
            if key not in current or not isinstance(current[key], MutableMapping):
                current[key] = {}
            current = current[key]
        else:
            raise ValueError(f"Cannot set pointer {pointer}; non-object encountered at {key}")
    last = parts[-1]
    if isinstance(current, MutableMapping):
        current[last] = value
        return
    raise ValueError(f"Cannot set pointer {pointer}; parent is not an object")


def _iter_flow_files(flows_dir: Path) -> list[Path]:
    if not flows_dir.exists():
        return []
    return sorted([p for p in flows_dir.glob("*.json") if p.is_file()])


def _parse_uuid_list_item(item: Any) -> dict[str, str] | None:
    if isinstance(item, str):
        text = item.strip()
        if not text:
            return None
        return {"id": text}
    if isinstance(item, Mapping):
        flow_id = _coerce_text(item.get("id") or item.get("uuid") or item.get("flow_uuid") or item.get("flow_id"))
        if not flow_id:
            return None
        version = _coerce_text(item.get("version") or item.get("base_version"))
        row = {"id": flow_id}
        if version:
            row["version"] = version
        return row
    return None


def load_uuid_list(path: Path) -> list[dict[str, str]]:
    suffix = path.suffix.lower()
    items: list[dict[str, str]] = []
    if suffix == ".json":
        data = _read_json(path)
        if isinstance(data, list):
            for item in data:
                parsed = _parse_uuid_list_item(item)
                if parsed:
                    items.append(parsed)
        elif isinstance(data, Mapping):
            for key in ("uuids", "ids", "data", "rows", "items"):
                if isinstance(data.get(key), list):
                    for item in data[key]:
                        parsed = _parse_uuid_list_item(item)
                        if parsed:
                            items.append(parsed)
                    break
    elif suffix == ".jsonl":
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            parsed = _parse_uuid_list_item(json.loads(text))
            if parsed:
                items.append(parsed)
    else:
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            parsed = _parse_uuid_list_item(text.split()[0])
            if parsed:
                items.append(parsed)
    # Deduplicate by (id, version)
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in items:
        key = (row["id"], row.get("version", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _repo_root_from_script() -> Path:
    return Path(__file__).resolve().parents[2]


def _process_builder_root() -> Path:
    return _repo_root_from_script() / "process-automated-builder"


def _lifecycleinventory_review_root() -> Path:
    return _repo_root_from_script() / "lifecycleinventory-review"


def _lifecycleinventory_review_entrypoint() -> Path:
    return _lifecycleinventory_review_root() / "scripts" / "run_review.py"


def _ensure_process_builder_on_syspath() -> None:
    pb_root = _process_builder_root()
    if not pb_root.exists():
        raise RuntimeError(f"process-automated-builder not found: {pb_root}")
    pb_text = str(pb_root)
    if pb_text not in sys.path:
        sys.path.insert(0, pb_text)


def _run_lifecycleinventory_review_flow(
    *,
    flows_dir: Path,
    out_dir: Path,
    run_root: Path | None = None,
    run_id: str | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    logic_version: str | None = None,
    enable_llm: bool = False,
    disable_llm: bool = False,
    llm_model: str | None = None,
    llm_max_flows: int | None = None,
    llm_batch_size: int | None = None,
    with_reference_context: bool = False,
    similarity_threshold: float | None = None,
) -> dict[str, Any]:
    entry = _lifecycleinventory_review_entrypoint()
    if not entry.exists():
        raise RuntimeError(f"lifecycleinventory-review entrypoint not found: {entry}")

    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(entry),
        "--profile",
        "flow",
        "--flows-dir",
        str(flows_dir),
        "--out-dir",
        str(out_dir),
    ]
    if run_root is not None:
        cmd += ["--run-root", str(run_root)]
    if run_id:
        cmd += ["--run-id", str(run_id)]
    if start_ts:
        cmd += ["--start-ts", str(start_ts)]
    if end_ts:
        cmd += ["--end-ts", str(end_ts)]
    if logic_version:
        cmd += ["--logic-version", str(logic_version)]
    if enable_llm and disable_llm:
        raise RuntimeError("Cannot set both enable_llm and disable_llm for lifecycleinventory-review flow call")
    if enable_llm:
        cmd += ["--enable-llm"]
    elif disable_llm:
        cmd += ["--disable-llm"]
    if llm_model:
        cmd += ["--llm-model", str(llm_model)]
    if llm_max_flows is not None:
        cmd += ["--llm-max-flows", str(llm_max_flows)]
    if llm_batch_size is not None:
        cmd += ["--llm-batch-size", str(llm_batch_size)]
    if with_reference_context:
        cmd += ["--with-reference-context"]
    if similarity_threshold is not None:
        cmd += ["--similarity-threshold", str(similarity_threshold)]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        raise RuntimeError(
            "lifecycleinventory-review flow profile failed"
            + (f"; stdout={stdout[:1500]}" if stdout else "")
            + (f"; stderr={stderr[:1500]}" if stderr else "")
        )

    summary_path = out_dir / "flow_review_summary.json"
    if not summary_path.exists():
        raise RuntimeError(f"lifecycleinventory-review did not produce expected summary file: {summary_path}")
    summary = _read_json(summary_path)
    if not isinstance(summary, Mapping):
        raise RuntimeError(f"Unexpected flow review summary payload: {type(summary).__name__}")
    return dict(summary)


@dataclass
class McpCrudFacade:
    """Thin facade reusing process-automated-builder MCP + CRUD services."""

    _crud: Any
    _mcp: Any
    _settings: Any

    @classmethod
    def create(cls) -> "McpCrudFacade":
        _ensure_process_builder_on_syspath()
        from tiangong_lca_spec.core.config import get_settings
        from tiangong_lca_spec.core.mcp_client import MCPToolClient
        from tiangong_lca_spec.publishing.crud import DatabaseCrudClient

        settings = get_settings()
        mcp = MCPToolClient(settings)
        crud = DatabaseCrudClient(settings, mcp_client=mcp)
        return cls(_crud=crud, _mcp=mcp, _settings=settings)

    @property
    def server_name(self) -> str:
        return str(self._settings.flow_search_service_name)

    def close(self) -> None:
        try:
            self._crud.close()
        except Exception:
            # DatabaseCrudClient closes the shared MCP client; best effort only.
            pass

    def select_flow(self, flow_uuid: str, version: str | None = None) -> dict[str, Any] | None:
        dataset = self._crud.select_flow(flow_uuid, version=version)
        if isinstance(dataset, Mapping):
            return {"flowDataSet": copy.deepcopy(dict(dataset))}
        return None

    def select_flow_record(self, flow_uuid: str) -> dict[str, Any] | None:
        result = self._crud.select_flow_record(flow_uuid)
        return copy.deepcopy(result) if isinstance(result, Mapping) else None

    def insert_flow(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._crud.insert_flow(payload)

    def _raw_crud(self, payload: Mapping[str, Any]) -> Any:
        return self._mcp.invoke_json_tool(self.server_name, DATABASE_TOOL_NAME, payload)

    def select_table_dataset(
        self,
        table: str,
        record_id: str,
        *,
        version: str | None = None,
        root_key: str | None = None,
    ) -> dict[str, Any] | None:
        payload: dict[str, Any] = {"operation": "select", "table": table, "id": record_id}
        if version:
            payload["version"] = version
        raw = self._raw_crud(payload)
        if not isinstance(raw, Mapping):
            return None
        if root_key and isinstance(raw.get(root_key), Mapping):
            return {root_key: copy.deepcopy(dict(raw[root_key]))}
        data = raw.get("data")
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, Mapping):
                for key in ("json_ordered", "json"):
                    blob = first.get(key)
                    if isinstance(blob, Mapping):
                        if root_key and isinstance(blob.get(root_key), Mapping):
                            return {root_key: copy.deepcopy(dict(blob[root_key]))}
                        if not root_key:
                            return copy.deepcopy(dict(blob))
        return None

    def select_flowproperty(self, fp_uuid: str, version: str | None = None) -> dict[str, Any] | None:
        return self.select_table_dataset("flowproperties", fp_uuid, version=version, root_key="flowPropertyDataSet")

    def select_unitgroup(self, ug_uuid: str, version: str | None = None) -> dict[str, Any] | None:
        return self.select_table_dataset("unitgroups", ug_uuid, version=version, root_key="unitGroupDataSet")


def _finding(
    *,
    flow_uuid: str,
    base_version: str,
    severity: str,
    rule_id: str,
    message: str,
    evidence: Mapping[str, Any] | None = None,
    fixability: str = "manual",
    suggested_action: str | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "flow_uuid": flow_uuid,
        "base_version": base_version,
        "severity": severity,
        "rule_id": rule_id,
        "message": message,
        "fixability": fixability,
    }
    if evidence:
        item["evidence"] = dict(evidence)
    if suggested_action:
        item["suggested_action"] = suggested_action
    return item


def _review_one_flow(
    doc: Mapping[str, Any],
    *,
    mcp: McpCrudFacade | None = None,
    fp_cache: dict[tuple[str, str], dict[str, Any] | None] | None = None,
    ug_cache: dict[str, dict[str, Any] | None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    flow_ds = _flow_root(doc)
    flow_uuid = _flow_uuid(flow_ds) or "(missing-uuid)"
    version = _flow_version(flow_ds)
    flow_type = _flow_type(flow_ds)
    names = _name_texts(flow_ds)
    leaf = _classification_leaf(flow_ds)

    summary: dict[str, Any] = {
        "flow_uuid": flow_uuid,
        "base_version": version,
        "type_of_dataset": flow_type,
        "classification_leaf": leaf,
        "name_primary_en": _name_primary(flow_ds, "en"),
        "name_primary_zh": _name_primary(flow_ds, "zh"),
        "name_fingerprint": _name_fingerprint(flow_ds),
        "flow_property_uuid": "",
        "flow_property_version": "",
        "flow_property_internal_id": "",
        "unitgroup_uuid": "",
        "unitgroup_reference_unit_name": "",
    }

    if not flow_uuid or flow_uuid == "(missing-uuid)":
        findings.append(
            _finding(
                flow_uuid=flow_uuid,
                base_version=version,
                severity="error",
                rule_id="missing_flow_uuid",
                message="Flow missing common:UUID.",
                fixability="manual",
            )
        )

    if not version:
        findings.append(
            _finding(
                flow_uuid=flow_uuid,
                base_version=version,
                severity="warning",
                rule_id="missing_dataset_version",
                message="Flow missing administrativeInformation.publicationAndOwnership.common:dataSetVersion.",
                fixability="auto",
                suggested_action="Set a valid ILCD version before publish.",
            )
        )

    if not flow_type:
        findings.append(
            _finding(
                flow_uuid=flow_uuid,
                base_version=version,
                severity="error",
                rule_id="missing_type_of_dataset",
                message="typeOfDataSet is missing under modellingAndValidation.LCIMethod.",
                fixability="manual",
            )
        )
    elif _is_elementary_flow(flow_type):
        findings.append(
            _finding(
                flow_uuid=flow_uuid,
                base_version=version,
                severity="error",
                rule_id="unexpected_elementary_flow",
                message="Flow is Elementary flow but this pipeline is intended for non-elementary flow remediation.",
                evidence={"typeOfDataSet": flow_type},
                fixability="manual",
            )
        )

    if not names:
        findings.append(
            _finding(
                flow_uuid=flow_uuid,
                base_version=version,
                severity="warning",
                rule_id="missing_name_text",
                message="No textual name fields found under flowInformation.dataSetInformation.name.",
                fixability="manual",
            )
        )
    else:
        if any("emergy" in text.lower() for text in names):
            findings.append(
                _finding(
                    flow_uuid=flow_uuid,
                    base_version=version,
                    severity="warning",
                    rule_id="name_contains_emergy",
                    message="Name subtree contains 'Emergy'.",
                    evidence={"matched_count": sum(1 for text in names if "emergy" in text.lower())},
                    fixability="manual",
                )
            )

    if not leaf["key"]:
        findings.append(
            _finding(
                flow_uuid=flow_uuid,
                base_version=version,
                severity="warning",
                rule_id="missing_classification_leaf",
                message="Classification leaf is missing.",
                fixability="manual",
            )
        )

    prop, chosen_internal_id = _pick_reference_flow_property(flow_ds)
    quant_ref_id = _quant_ref_internal_id(flow_ds)
    if prop is None:
        findings.append(
            _finding(
                flow_uuid=flow_uuid,
                base_version=version,
                severity="error",
                rule_id="missing_flow_property",
                message="No flowProperties.flowProperty entry found.",
                fixability="manual",
            )
        )
    else:
        pref = _flow_property_ref(prop)
        summary["flow_property_uuid"] = pref["uuid"]
        summary["flow_property_version"] = pref["version"]
        summary["flow_property_internal_id"] = pref["internal_id"]

        if not pref["uuid"]:
            findings.append(
                _finding(
                    flow_uuid=flow_uuid,
                    base_version=version,
                    severity="error",
                    rule_id="invalid_flow_property_reference",
                    message="referenceToFlowPropertyDataSet does not contain a parseable UUID.",
                    evidence={"internal_id": pref["internal_id"], "short_name": pref["short_name"]},
                    fixability="manual",
                )
            )

        if not quant_ref_id:
            findings.append(
                _finding(
                    flow_uuid=flow_uuid,
                    base_version=version,
                    severity="warning",
                    rule_id="missing_quantitative_reference",
                    message="referenceToReferenceFlowProperty is missing.",
                    evidence={"expected_internal_id": chosen_internal_id or pref["internal_id"]},
                    fixability="auto",
                    suggested_action="Set quantitative reference to the chosen flowProperty @dataSetInternalID.",
                )
            )
        elif chosen_internal_id and quant_ref_id != chosen_internal_id:
            findings.append(
                _finding(
                    flow_uuid=flow_uuid,
                    base_version=version,
                    severity="warning",
                    rule_id="quantitative_reference_mismatch",
                    message="Quantitative reference internal ID does not match the chosen reference flowProperty.",
                    evidence={"quant_ref_internal_id": quant_ref_id, "expected_internal_id": chosen_internal_id},
                    fixability="auto",
                    suggested_action="Align quantitative reference with chosen flowProperty internal ID.",
                )
            )

        if mcp and pref["uuid"]:
            fp_key = (pref["uuid"], pref["version"])
            fp_doc = None
            if fp_cache is not None and fp_key in fp_cache:
                fp_doc = fp_cache[fp_key]
            else:
                fp_doc = mcp.select_flowproperty(pref["uuid"], pref["version"] or None) or mcp.select_flowproperty(pref["uuid"])
                if fp_cache is not None:
                    fp_cache[fp_key] = fp_doc
            if not fp_doc:
                findings.append(
                    _finding(
                        flow_uuid=flow_uuid,
                        base_version=version,
                        severity="warning",
                        rule_id="flowproperty_lookup_failed",
                        message="Could not fetch referenced flow property dataset via MCP CRUD.",
                        evidence={"flow_property_uuid": pref["uuid"], "flow_property_version": pref["version"]},
                        fixability="manual",
                    )
                )
            else:
                ug_uuid = _extract_flowproperty_unitgroup_ref(fp_doc)
                summary["unitgroup_uuid"] = ug_uuid
                if not ug_uuid:
                    findings.append(
                        _finding(
                            flow_uuid=flow_uuid,
                            base_version=version,
                            severity="warning",
                            rule_id="missing_unitgroup_reference",
                            message="Referenced flow property does not expose a parseable unit group UUID.",
                            evidence={"flow_property_uuid": pref["uuid"]},
                            fixability="manual",
                        )
                    )
                else:
                    ug_doc = None
                    if ug_cache is not None and ug_uuid in ug_cache:
                        ug_doc = ug_cache[ug_uuid]
                    else:
                        ug_doc = mcp.select_unitgroup(ug_uuid)
                        if ug_cache is not None:
                            ug_cache[ug_uuid] = ug_doc
                    if not ug_doc:
                        findings.append(
                            _finding(
                                flow_uuid=flow_uuid,
                                base_version=version,
                                severity="warning",
                                rule_id="unitgroup_lookup_failed",
                                message="Could not fetch unit group dataset via MCP CRUD.",
                                evidence={"unitgroup_uuid": ug_uuid},
                                fixability="manual",
                            )
                        )
                    else:
                        ref_unit = _extract_reference_unit_name(ug_doc)
                        summary["unitgroup_reference_unit_name"] = ref_unit
                        if not ref_unit:
                            findings.append(
                                _finding(
                                    flow_uuid=flow_uuid,
                                    base_version=version,
                                    severity="warning",
                                    rule_id="missing_reference_unit_name",
                                    message="Unit group does not expose a readable reference unit name.",
                                    evidence={"unitgroup_uuid": ug_uuid},
                                    fixability="manual",
                                )
                            )
                        else:
                            # Very light heuristic: keep this as warning-only to avoid false confidence.
                            if flow_type and "product" in flow_type.lower():
                                token = ref_unit.strip().lower()
                                if token in {"kg", "g", "t", "m3", "l", "piece", "pcs", "pc", "ea", "mj", "kwh", "m2", "m"}:
                                    pass
                                else:
                                    findings.append(
                                        _finding(
                                            flow_uuid=flow_uuid,
                                            base_version=version,
                                            severity="warning",
                                            rule_id="unitgroup_needs_review",
                                            message="Reference unit is uncommon for Product flow; review flowProperty/unitgroup selection.",
                                            evidence={
                                                "typeOfDataSet": flow_type,
                                                "flow_property_uuid": pref["uuid"],
                                                "unitgroup_uuid": ug_uuid,
                                                "reference_unit_name": ref_unit,
                                            },
                                            fixability="manual",
                                        )
                                    )

    return findings, summary


def _similarity_findings(
    summaries: list[dict[str, Any]],
    *,
    threshold: float = 0.92,
    max_pairs_per_group: int = 20000,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    findings: list[dict[str, Any]] = []
    pairs: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in summaries:
        leaf_key = _coerce_text((row.get("classification_leaf") or {}).get("key") if isinstance(row.get("classification_leaf"), Mapping) else "")
        if not leaf_key:
            continue
        fp_uuid = _coerce_text(row.get("flow_property_uuid"))
        ug_uuid = _coerce_text(row.get("unitgroup_uuid"))
        name_fp = _coerce_text(row.get("name_fingerprint"))
        if not name_fp:
            continue
        grouped[(leaf_key, fp_uuid, ug_uuid)].append(row)

    for group_key, rows in grouped.items():
        if len(rows) < 2:
            continue
        pair_count = 0
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                if pair_count >= max_pairs_per_group:
                    break
                left = rows[i]
                right = rows[j]
                a = _coerce_text(left.get("name_fingerprint"))
                b = _coerce_text(right.get("name_fingerprint"))
                if not a or not b:
                    continue
                ratio = SequenceMatcher(None, a, b).ratio()
                pair_count += 1
                if ratio < threshold:
                    continue
                pair = {
                    "classification_group": group_key[0],
                    "flow_property_uuid": group_key[1],
                    "unitgroup_uuid": group_key[2],
                    "left_flow_uuid": left.get("flow_uuid"),
                    "right_flow_uuid": right.get("flow_uuid"),
                    "left_version": left.get("base_version"),
                    "right_version": right.get("base_version"),
                    "similarity": round(ratio, 6),
                    "left_name_en": left.get("name_primary_en"),
                    "right_name_en": right.get("name_primary_en"),
                }
                pairs.append(pair)
                for current, other in ((left, right), (right, left)):
                    findings.append(
                        _finding(
                            flow_uuid=_coerce_text(current.get("flow_uuid")),
                            base_version=_coerce_text(current.get("base_version")),
                            severity="warning",
                            rule_id="same_category_high_similarity",
                            message="Another flow in the same classification/flowProperty/unitgroup group is highly similar.",
                            evidence={
                                "other_flow_uuid": _coerce_text(other.get("flow_uuid")),
                                "other_version": _coerce_text(other.get("base_version")),
                                "similarity": round(ratio, 6),
                                "classification_group": group_key[0],
                            },
                            fixability="review-needed",
                            suggested_action="Review duplicate/near-duplicate risk before automated changes.",
                        )
                    )
            if pair_count >= max_pairs_per_group:
                break
    return findings, pairs


def _review_directory(
    flows_dir: Path,
    out_dir: Path,
    *,
    with_reference_context: bool = False,
    similarity_threshold: float = 0.92,
    max_pairs_per_group: int = 20000,
) -> dict[str, Any]:
    flow_files = _iter_flow_files(flows_dir)
    if not flow_files:
        raise RuntimeError(f"No flow JSON files found in {flows_dir}")

    mcp: McpCrudFacade | None = None
    fp_cache: dict[tuple[str, str], dict[str, Any] | None] = {}
    ug_cache: dict[str, dict[str, Any] | None] = {}
    if with_reference_context:
        mcp = McpCrudFacade.create()

    try:
        all_findings: list[dict[str, Any]] = []
        summaries: list[dict[str, Any]] = []
        per_flow: list[dict[str, Any]] = []
        for path in flow_files:
            doc = _read_json(path)
            findings, summary = _review_one_flow(doc, mcp=mcp, fp_cache=fp_cache, ug_cache=ug_cache)
            summary["source_file"] = path.name
            summaries.append(summary)
            per_flow.append(
                {
                    "flow_uuid": summary.get("flow_uuid"),
                    "base_version": summary.get("base_version"),
                    "source_file": path.name,
                    "finding_count": len(findings),
                }
            )
            all_findings.extend(findings)

        sim_findings, sim_pairs = _similarity_findings(
            summaries,
            threshold=similarity_threshold,
            max_pairs_per_group=max_pairs_per_group,
        )
        all_findings.extend(sim_findings)

        out_dir.mkdir(parents=True, exist_ok=True)
        _write_jsonl(out_dir / "findings.jsonl", all_findings)
        _write_jsonl(out_dir / "flow_summaries.jsonl", summaries)
        _write_jsonl(out_dir / "similarity_pairs.jsonl", sim_pairs)
        _write_json(out_dir / "review_index.json", per_flow)

        severity_counts: dict[str, int] = defaultdict(int)
        rule_counts: dict[str, int] = defaultdict(int)
        fixability_counts: dict[str, int] = defaultdict(int)
        for item in all_findings:
            severity_counts[_coerce_text(item.get("severity")) or "unknown"] += 1
            rule_counts[_coerce_text(item.get("rule_id")) or "unknown"] += 1
            fixability_counts[_coerce_text(item.get("fixability")) or "unknown"] += 1
        summary = {
            "flow_count": len(flow_files),
            "finding_count": len(all_findings),
            "similarity_pair_count": len(sim_pairs),
            "with_reference_context": with_reference_context,
            "severity_counts": dict(sorted(severity_counts.items())),
            "fixability_counts": dict(sorted(fixability_counts.items())),
            "rule_counts": dict(sorted(rule_counts.items())),
        }
        _write_json(out_dir / "review_summary.json", summary)
        return summary
    finally:
        if mcp is not None:
            mcp.close()


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        value = json.loads(text)
        if isinstance(value, Mapping):
            rows.append(dict(value))
    return rows


def _build_flow_file_index(flows_dir: Path) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for path in _iter_flow_files(flows_dir):
        try:
            doc = _read_json(path)
        except Exception:
            continue
        uuid_value = _flow_uuid(_flow_root(doc))
        if uuid_value:
            index[uuid_value] = path
    return index


def _summarize_findings_by_flow(findings: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in findings:
        flow_uuid = _coerce_text(item.get("flow_uuid"))
        if not flow_uuid:
            continue
        grouped[flow_uuid].append(item)
    return grouped


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = _coerce_text(value).lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def _set_flow_uuid(flow_ds: MutableMapping[str, Any], value: str) -> None:
    flow_ds.setdefault("flowInformation", {})
    info = flow_ds["flowInformation"]
    if not isinstance(info, MutableMapping):
        flow_ds["flowInformation"] = {}
        info = flow_ds["flowInformation"]
    info.setdefault("dataSetInformation", {})
    data_info = info["dataSetInformation"]
    if not isinstance(data_info, MutableMapping):
        info["dataSetInformation"] = {}
        data_info = info["dataSetInformation"]
    data_info["common:UUID"] = value


def _flow_classification_node(flow_ds: Mapping[str, Any]) -> Any:
    return _deep_get(
        flow_ds,
        (
            "flowInformation",
            "dataSetInformation",
            "classificationInformation",
            "common:classification",
            "common:class",
        ),
    )


def _classification_changed(before_doc: Mapping[str, Any], after_doc: Mapping[str, Any]) -> bool:
    before_classes = _as_list(_flow_classification_node(_flow_root(before_doc)))
    after_classes = _as_list(_flow_classification_node(_flow_root(after_doc)))
    return _sha256_json(before_classes) != _sha256_json(after_classes)


def _parse_llm_json(raw: str) -> dict[str, Any]:
    cleaned = raw.strip()
    if not cleaned:
        raise ValueError("empty_llm_response")
    try:
        _ensure_process_builder_on_syspath()
        from tiangong_lca_spec.core.json_utils import parse_json_response

        parsed = parse_json_response(cleaned)
        if isinstance(parsed, Mapping):
            return dict(parsed)
    except Exception:
        pass
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, Mapping):
            return dict(parsed)
    except Exception:
        pass
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        parsed = json.loads(cleaned[start : end + 1])
        if isinstance(parsed, Mapping):
            return dict(parsed)
    raise ValueError("non_json_llm_response")


def _validate_ilcd_flow_schema(flow_doc: Mapping[str, Any]) -> tuple[bool, str]:
    wrapper = _flow_wrapper(flow_doc)
    try:
        from tidas_sdk import create_flow

        create_flow(wrapper, validate=True)
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _issue_payload(finding: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "rule_id": _coerce_text(finding.get("rule_id")),
        "message": _coerce_text(finding.get("message")),
        "severity": _coerce_text(finding.get("severity")),
        "fixability": _coerce_text(finding.get("fixability")),
    }
    evidence = finding.get("evidence")
    if isinstance(evidence, Mapping):
        payload["evidence"] = dict(evidence)
    suggestion = _coerce_text(finding.get("suggested_action") or finding.get("action") or finding.get("suggestion"))
    if suggestion:
        payload["suggestion"] = suggestion
    return payload


def _build_remediation_constraints() -> dict[str, Any]:
    return {
        "must_follow_schema": "patched_flow_json must conform to ILCD FlowDataSet schema.",
        "classification_policy": (
            "Do not directly finalize classification/category values in patched_flow_json when classification/name/category "
            "rebuild is needed. Set needs_regen_service=true and provide classification intent/candidates only."
        ),
        "minimal_change": "Prefer minimal patch scope for this issue only.",
        "allow_no_change": True,
    }


def _build_remediation_contract(
    *,
    flow_uuid: str,
    base_version: str,
    flow_doc: Mapping[str, Any],
    finding: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "flow_uuid": flow_uuid,
        "base_version": base_version,
        "original_flow_json": _flow_wrapper(flow_doc),
        "issue": _issue_payload(finding),
        "constraints": _build_remediation_constraints(),
    }


def _build_llm_remediation_prompt(contract: Mapping[str, Any]) -> str:
    return (
        "You are an ILCD flow remediation assistant.\n"
        "Return strict JSON only with keys:\n"
        "- modified: true|false\n"
        "- reason: string\n"
        "- patched_flow_json: object|null\n"
        "- changes: [{path,before,after,rationale}]\n"
        "- needs_regen_service: true|false\n"
        "Optional extra key allowed: classification_intent (object|string|array).\n\n"
        "Rules:\n"
        "1) Follow ILCD flow schema.\n"
        "2) Make minimal changes scoped to the issue.\n"
        "3) If unsure, set modified=false.\n"
        "4) If classification/category/name rebuild is needed, set needs_regen_service=true.\n"
        "5) Never output natural-language explanations outside JSON.\n\n"
        f"Input:\n{json.dumps(contract, ensure_ascii=False)}"
    )


def _openai_chat_json(
    *,
    system_prompt: str,
    user_prompt: str,
    model: str,
    api_key: str,
    base_url: str,
    timeout_sec: int,
) -> dict[str, Any]:
    if not api_key:
        return {"ok": False, "error": "OPENAI_API_KEY missing"}
    token = api_key.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    endpoint = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    req = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=max(10, int(timeout_sec))) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        return {"ok": False, "error": "llm_missing_choices", "raw_body": body}
    content = choices[0].get("message", {}).get("content")
    text = _coerce_text(content)
    if not text:
        return {"ok": False, "error": "llm_empty_content", "raw_body": body}
    try:
        parsed = _parse_llm_json(text)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"llm_non_json:{exc}", "raw": text}
    return {"ok": True, "parsed": parsed, "raw": text}


def _normalize_remediation_changes(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _as_list(value):
        if not isinstance(item, Mapping):
            continue
        path = _coerce_text(item.get("path"))
        if not path:
            continue
        rows.append(
            {
                "path": path,
                "before": item.get("before"),
                "after": item.get("after"),
                "rationale": _coerce_text(item.get("rationale") or item.get("reason")),
            }
        )
    return rows


def _normalize_llm_remediation_result(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(raw) if isinstance(raw, Mapping) else {}
    patched = data.get("patched_flow_json")
    patched_json: dict[str, Any] | None = None
    if isinstance(patched, Mapping):
        patched_json = copy.deepcopy(dict(patched))
    elif isinstance(patched, str) and patched.strip():
        try:
            parsed = _parse_llm_json(patched)
            patched_json = parsed
        except Exception:
            patched_json = None
    return {
        "modified": _coerce_bool(data.get("modified"), default=False),
        "reason": _coerce_text(data.get("reason")),
        "patched_flow_json": patched_json,
        "changes": _normalize_remediation_changes(data.get("changes")),
        "needs_regen_service": _coerce_bool(data.get("needs_regen_service"), default=False),
        "classification_intent": data.get("classification_intent"),
    }


def _issue_needs_regen(finding: Mapping[str, Any]) -> bool:
    text = " ".join(
        [
            _coerce_text(finding.get("rule_id")),
            _coerce_text(finding.get("message")),
            _coerce_text(finding.get("suggested_action")),
        ]
    ).lower()
    keywords = [
        "classification",
        "category",
        "class_id",
        "classid",
        "name",
        "base_name",
        "mixandlocationtypes",
        "treatmentstandardsroutes",
        "分类",
        "类别",
        "名称",
    ]
    return any(token in text for token in keywords)


def _split_terms(value: str) -> list[str]:
    if not value:
        return []
    raw = value.replace("；", ";")
    out: list[str] = []
    for chunk in raw.split(";"):
        for piece in chunk.split(","):
            token = piece.strip()
            if token:
                out.append(token)
    return out


def _collect_hint_terms(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _split_terms(value)
    if isinstance(value, Mapping):
        out: list[str] = []
        for item in value.values():
            out.extend(_collect_hint_terms(item))
        return out
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(_collect_hint_terms(item))
        return out
    text = _coerce_text(value)
    return _split_terms(text)


def _collect_classification_hints(
    *,
    finding: Mapping[str, Any],
    llm_result: Mapping[str, Any],
    candidate_doc: Mapping[str, Any] | None,
) -> list[str]:
    hints: list[str] = []
    hints.extend(_collect_hint_terms(llm_result.get("classification_intent")))
    hints.extend(_collect_hint_terms(llm_result.get("reason")))
    hints.extend(_collect_hint_terms(finding.get("message")))
    hints.extend(_collect_hint_terms(finding.get("suggested_action")))
    for change in _as_list(llm_result.get("changes")):
        if not isinstance(change, Mapping):
            continue
        path = _coerce_text(change.get("path")).lower()
        if "classification" in path or "name" in path:
            hints.extend(_collect_hint_terms(change.get("after")))
            hints.extend(_collect_hint_terms(change.get("rationale")))
    if candidate_doc is not None:
        flow_ds = _flow_root(candidate_doc)
        hints.extend(_collect_hint_terms(_name_primary(flow_ds, "en")))
        hints.extend(_collect_hint_terms(_name_primary(flow_ds, "zh")))
        hints.extend(_collect_hint_terms([item.get("#text") for item in _classification_classes(flow_ds) if isinstance(item, Mapping)]))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in hints:
        text = item.strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(text)
        if len(deduped) >= 12:
            break
    return deduped


def _build_selector_exchange_and_hints(
    flow_doc: Mapping[str, Any],
    finding: Mapping[str, Any],
    hint_terms: list[str],
) -> tuple[dict[str, Any], dict[str, list[str] | str]]:
    flow_ds = _flow_root(flow_doc)
    comments = _deep_get(flow_ds, ("flowInformation", "dataSetInformation", "common:generalComment"))
    synonyms = _deep_get(flow_ds, ("flowInformation", "dataSetInformation", "common:synonyms"))
    exchange = {
        "exchangeName": _name_primary(flow_ds, "en") or _name_primary(flow_ds, "zh") or _flow_uuid(flow_ds),
        "exchangeDirection": "output",
        "generalComment": _lang_text(comments, "en") or _lang_text(comments, "zh"),
        "classificationInformation": {
            "common:classification": {
                "common:class": _classification_classes(flow_ds),
            }
        },
    }
    hints: dict[str, list[str] | str] = {
        "classification_hints": hint_terms,
        "en_synonyms": _split_terms(_lang_text(synonyms, "en")),
        "zh_synonyms": _split_terms(_lang_text(synonyms, "zh")),
        "usage_context": [_coerce_text(finding.get("rule_id")), _coerce_text(finding.get("message"))],
    }
    return exchange, hints


def _classification_entries_from_path(path: list[tuple[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for idx, (code, text) in enumerate(path):
        row = {"@level": str(idx), "#text": _coerce_text(text)}
        code_text = _coerce_text(code)
        if code_text:
            row["@classId"] = code_text
        rows.append(row)
    return rows


def _regenerate_flow_via_creation_service(
    *,
    flow_doc: Mapping[str, Any],
    finding: Mapping[str, Any],
    llm_result: Mapping[str, Any],
    candidate_doc: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _ensure_process_builder_on_syspath()
    from tiangong_lca_spec.product_flow_creation import ProductFlowCreateRequest, ProductFlowCreationService
    from tiangong_lca_spec.publishing.crud import FlowProductCategorySelector

    request_payload = _request_from_existing_flow(flow_doc)
    if candidate_doc is not None:
        candidate_request = _request_from_existing_flow(candidate_doc)
        for key in (
            "base_name_en",
            "base_name_zh",
            "treatment_en",
            "treatment_zh",
            "mix_en",
            "mix_zh",
            "comment_en",
            "comment_zh",
            "synonyms_en",
            "synonyms_zh",
        ):
            value = candidate_request.get(key)
            if isinstance(value, list):
                request_payload[key] = [item for item in value if _coerce_text(item)]
            elif _coerce_text(value):
                request_payload[key] = value

    hint_terms = _collect_classification_hints(
        finding=finding,
        llm_result=llm_result,
        candidate_doc=candidate_doc,
    )
    selected_path: list[tuple[str, str]] = []
    if _coerce_text(request_payload.get("flow_type")).lower() == "product flow":
        selector = FlowProductCategorySelector(llm=None)
        exchange, hints = _build_selector_exchange_and_hints(flow_doc, finding, hint_terms)
        selected_path = selector.select_path(exchange, hints)
        if selected_path:
            classification = _classification_entries_from_path(selected_path)
            request_payload["classification"] = classification
            request_payload["class_id"] = _coerce_text(classification[-1].get("@classId"))

    flow_ds = _flow_root(flow_doc)
    request_payload["flow_uuid"] = _flow_uuid(flow_ds)
    request_payload["version"] = _flow_version(flow_ds) or _coerce_text(request_payload.get("version")) or "01.01.000"
    request = ProductFlowCreateRequest(**request_payload)
    service = ProductFlowCreationService()
    result = service.build(request, allow_validation_fallback=False)
    meta = {
        "used_regen_service": True,
        "used_decision_tree": bool(selected_path),
        "selected_path": [
            {"code": _coerce_text(code), "text": _coerce_text(text)}
            for code, text in selected_path
        ],
        "hint_terms": hint_terms,
    }
    return result.payload, meta


def _llm_remediate_findings(
    flows_dir: Path,
    findings_path: Path,
    out_dir: Path,
    *,
    copy_unchanged: bool = False,
    llm_model: str | None = None,
    llm_base_url: str | None = None,
    llm_api_key: str | None = None,
    llm_timeout_sec: int = 120,
) -> dict[str, Any]:
    findings = _load_jsonl(findings_path)
    findings_by_flow = _summarize_findings_by_flow(findings)
    flow_files = _iter_flow_files(flows_dir)
    if not flow_files:
        raise RuntimeError(f"No flow JSON files found in {flows_dir}")

    resolved_model = (
        _coerce_text(llm_model)
        or _coerce_text(os.getenv("OPENAI_MODEL"))
        or _coerce_text(os.getenv("LCA_OPENAI_MODEL"))
        or "gpt-4o-mini"
    )
    resolved_base_url = (
        _coerce_text(llm_base_url)
        or _coerce_text(os.getenv("OPENAI_BASE_URL"))
        or _coerce_text(os.getenv("LCA_OPENAI_BASE_URL"))
        or "https://api.openai.com/v1"
    )
    resolved_api_key = (
        _coerce_text(llm_api_key)
        or _coerce_text(os.getenv("OPENAI_API_KEY"))
        or _coerce_text(os.getenv("LCA_OPENAI_API_KEY"))
    )
    llm_enabled = bool(resolved_api_key)

    patched_dir = out_dir / "patched_flows"
    patched_dir.mkdir(parents=True, exist_ok=True)

    actions: list[dict[str, Any]] = []
    modified_flags: list[dict[str, Any]] = []
    manifest: list[dict[str, Any]] = []
    changed_count = 0
    copied_count = 0
    issue_applied_count = 0
    issue_no_change_count = 0
    issue_error_count = 0

    system_prompt = (
        "You are a strict ILCD flow remediator. Output JSON only. "
        "Do not add markdown. Use the required keys exactly."
    )

    for flow_file in flow_files:
        doc = _read_json(flow_file)
        original_wrapper = _flow_wrapper(doc)
        working_wrapper = copy.deepcopy(original_wrapper)
        flow_ds = _flow_root(original_wrapper)
        flow_uuid = _flow_uuid(flow_ds)
        if not flow_uuid:
            continue
        base_version = _flow_version(flow_ds)
        flow_findings = findings_by_flow.get(flow_uuid, [])
        flow_applied_issues = 0

        for issue_idx, finding in enumerate(flow_findings, start=1):
            contract = _build_remediation_contract(
                flow_uuid=flow_uuid,
                base_version=base_version,
                flow_doc=working_wrapper,
                finding=finding,
            )
            llm_output = _normalize_llm_remediation_result(None)
            action_status = "no_change"
            schema_valid: bool | None = None
            schema_error = ""
            regen_meta: dict[str, Any] | None = None

            if llm_enabled:
                llm_call = _openai_chat_json(
                    system_prompt=system_prompt,
                    user_prompt=_build_llm_remediation_prompt(contract),
                    model=resolved_model,
                    api_key=resolved_api_key,
                    base_url=resolved_base_url,
                    timeout_sec=llm_timeout_sec,
                )
                if not bool(llm_call.get("ok")):
                    action_status = "llm_error"
                    schema_error = _coerce_text(llm_call.get("error"))
                else:
                    llm_output = _normalize_llm_remediation_result(llm_call.get("parsed"))
                    needs_regen_service = _coerce_bool(llm_output.get("needs_regen_service"), default=False) or _issue_needs_regen(
                        finding
                    )
                    candidate_doc: dict[str, Any] | None = None
                    if isinstance(llm_output.get("patched_flow_json"), Mapping):
                        candidate_doc = _flow_wrapper(llm_output["patched_flow_json"])

                    if _coerce_bool(llm_output.get("modified"), default=False):
                        if candidate_doc is None and not needs_regen_service:
                            action_status = "invalid_response_missing_patch"
                            schema_error = "modified=true but patched_flow_json is null"
                        else:
                            if candidate_doc is not None and _classification_changed(working_wrapper, candidate_doc):
                                needs_regen_service = True
                            if needs_regen_service:
                                try:
                                    candidate_doc, regen_meta = _regenerate_flow_via_creation_service(
                                        flow_doc=working_wrapper,
                                        finding=finding,
                                        llm_result=llm_output,
                                        candidate_doc=candidate_doc,
                                    )
                                except Exception as exc:  # noqa: BLE001
                                    candidate_doc = None
                                    action_status = "regen_failed"
                                    schema_error = str(exc)
                            if candidate_doc is not None:
                                candidate_flow_ds = _flow_root(candidate_doc)
                                _set_flow_uuid(candidate_flow_ds, flow_uuid)
                                if base_version:
                                    _set_flow_version(candidate_flow_ds, base_version)
                                schema_valid, schema_error = _validate_ilcd_flow_schema(candidate_doc)
                                if schema_valid:
                                    if _sha256_json(candidate_doc) != _sha256_json(working_wrapper):
                                        working_wrapper = candidate_doc
                                        flow_applied_issues += 1
                                        issue_applied_count += 1
                                        action_status = "applied"
                                    else:
                                        action_status = "no_effect"
                                else:
                                    action_status = "schema_failed"
                    else:
                        action_status = "no_change"
            else:
                action_status = "llm_unavailable"
                schema_error = "OPENAI_API_KEY missing"

            if action_status in {"applied"}:
                pass
            elif action_status in {"no_change", "no_effect", "llm_unavailable"}:
                issue_no_change_count += 1
            else:
                issue_error_count += 1

            action_row: dict[str, Any] = {
                "flow_uuid": flow_uuid,
                "base_version": base_version,
                "finding_index": issue_idx,
                "issue": _issue_payload(finding),
                "input_contract": contract,
                "llm_output": llm_output,
                "status": action_status,
                "modified_requested": _coerce_bool(llm_output.get("modified"), default=False),
                "modified_applied": action_status == "applied",
                "needs_regen_service": _coerce_bool(llm_output.get("needs_regen_service"), default=False),
            }
            if schema_valid is not None:
                action_row["schema_valid"] = schema_valid
            if schema_error:
                action_row["schema_error"] = schema_error
            if regen_meta:
                action_row["regen_meta"] = regen_meta
            actions.append(action_row)

        changed = _sha256_json(working_wrapper) != _sha256_json(original_wrapper)
        final_schema_valid, final_schema_error = _validate_ilcd_flow_schema(working_wrapper if changed else original_wrapper)
        out_path = patched_dir / flow_file.name

        if not changed and not copy_unchanged:
            modified_flags.append(
                {
                    "flow_uuid": flow_uuid,
                    "base_version": base_version,
                    "modified": False,
                    "finding_count": len(flow_findings),
                    "applied_issue_count": flow_applied_issues,
                    "schema_valid": final_schema_valid,
                }
            )
            continue

        if changed:
            _write_json(out_path, working_wrapper)
            changed_count += 1
        else:
            _write_json(out_path, original_wrapper)
            copied_count += 1

        patched_root = _flow_root(working_wrapper if changed else original_wrapper)
        manifest.append(
            {
                "flow_uuid": flow_uuid,
                "base_version": base_version,
                "patched_version_before_publish": _flow_version(patched_root),
                "source_file": str(flow_file),
                "patched_file": str(out_path),
                "changed": changed,
                "schema_valid": final_schema_valid,
                "schema_error": final_schema_error,
                "before_sha256": _sha256_json(original_wrapper),
                "before_sha256_no_version": _sha256_flow_without_version(original_wrapper),
                "after_sha256": _sha256_json(working_wrapper if changed else original_wrapper),
                "after_sha256_no_version": _sha256_flow_without_version(working_wrapper if changed else original_wrapper),
            }
        )
        modified_flags.append(
            {
                "flow_uuid": flow_uuid,
                "base_version": base_version,
                "modified": changed,
                "finding_count": len(flow_findings),
                "applied_issue_count": flow_applied_issues,
                "schema_valid": final_schema_valid,
                "schema_error": final_schema_error,
                "patched_file": str(out_path),
            }
        )

    _write_jsonl(out_dir / "remediation_actions.jsonl", actions)
    _write_jsonl(out_dir / "modified_flags.jsonl", modified_flags)
    _write_jsonl(out_dir / "patch_manifest.jsonl", manifest)
    summary = {
        "llm_enabled": llm_enabled,
        "llm_model": resolved_model,
        "input_flow_count": len(flow_files),
        "flows_with_findings": len(findings_by_flow),
        "finding_count": len(findings),
        "remediation_action_count": len(actions),
        "patch_manifest_count": len(manifest),
        "changed_count": changed_count,
        "copied_unchanged_count": copied_count,
        "issue_applied_count": issue_applied_count,
        "issue_no_change_count": issue_no_change_count,
        "issue_error_count": issue_error_count,
    }
    _write_json(out_dir / "remediation_summary.json", summary)
    _write_json(out_dir / "fix_summary.json", summary)
    return summary


def _bump_versions_if_needed(
    manifest_path: Path,
    out_dir: Path,
    *,
    include_unchanged: bool = False,
) -> dict[str, Any]:
    rows = _load_jsonl(manifest_path)
    if not rows:
        summary = {"row_count": 0, "changed_count": 0, "bumped_count": 0, "error_count": 0, "skipped": True}
        _write_jsonl(out_dir / "version_bump_log.jsonl", [])
        _write_json(out_dir / "version_bump_summary.json", summary)
        return summary

    updated_rows: list[dict[str, Any]] = []
    logs: list[dict[str, Any]] = []
    changed_count = 0
    bumped_count = 0
    error_count = 0

    for row in rows:
        item = dict(row)
        changed = bool(item.get("changed"))
        flow_uuid = _coerce_text(item.get("flow_uuid"))
        patched_file = Path(_coerce_text(item.get("patched_file")))
        base_version = _coerce_text(item.get("base_version"))
        if not changed and not include_unchanged:
            logs.append(
                {
                    "flow_uuid": flow_uuid,
                    "base_version": base_version,
                    "status": "skipped_unchanged",
                    "bumped": False,
                }
            )
            updated_rows.append(item)
            continue
        changed_count += 1
        if not patched_file.exists():
            logs.append(
                {
                    "flow_uuid": flow_uuid,
                    "base_version": base_version,
                    "patched_file": str(patched_file),
                    "status": "error",
                    "reason": "patched_file_missing",
                    "bumped": False,
                }
            )
            error_count += 1
            updated_rows.append(item)
            continue

        wrapper = _flow_wrapper(_read_json(patched_file))
        flow_ds = wrapper["flowDataSet"]
        before_version = _flow_version(flow_ds)
        expected_version = _bump_ilcd_version(base_version or before_version or "01.01.000")
        bumped = before_version != expected_version
        if bumped:
            _set_flow_version(flow_ds, expected_version)
            _write_json(patched_file, wrapper)
            bumped_count += 1
        after_version = _flow_version(flow_ds)

        item["patched_version_before_publish"] = after_version
        item["version_bumped"] = bumped
        logs.append(
            {
                "flow_uuid": flow_uuid,
                "base_version": base_version,
                "before_version": before_version,
                "expected_version": expected_version,
                "after_version": after_version,
                "patched_file": str(patched_file),
                "status": "ok",
                "bumped": bumped,
            }
        )
        updated_rows.append(item)

    _write_jsonl(manifest_path, updated_rows)
    _write_jsonl(out_dir / "version_bump_log.jsonl", logs)
    summary = {
        "row_count": len(rows),
        "changed_count": changed_count,
        "bumped_count": bumped_count,
        "error_count": error_count,
    }
    _write_json(out_dir / "version_bump_summary.json", summary)
    return summary


def _validate_schema_outputs(
    manifest_path: Path,
    out_dir: Path,
    *,
    include_unchanged: bool = True,
) -> dict[str, Any]:
    rows = _load_jsonl(manifest_path)
    if not rows:
        summary = {"row_count": 0, "validated_count": 0, "schema_valid_count": 0, "schema_invalid_count": 0, "skipped": True}
        _write_jsonl(out_dir / "schema_validation.jsonl", [])
        _write_json(out_dir / "schema_validation_summary.json", summary)
        return summary

    schema_valid_dir = out_dir / "schema_valid_flows"
    schema_valid_dir.mkdir(parents=True, exist_ok=True)

    updated_rows: list[dict[str, Any]] = []
    reports: list[dict[str, Any]] = []
    validated_count = 0
    valid_count = 0
    invalid_count = 0

    for row in rows:
        item = dict(row)
        changed = bool(item.get("changed"))
        flow_uuid = _coerce_text(item.get("flow_uuid"))
        patched_file = Path(_coerce_text(item.get("patched_file")))

        if not changed and not include_unchanged:
            item["schema_valid"] = False
            item["schema_error"] = "skipped_unchanged"
            reports.append(
                {
                    "flow_uuid": flow_uuid,
                    "patched_file": str(patched_file),
                    "status": "skipped_unchanged",
                    "schema_valid": False,
                }
            )
            updated_rows.append(item)
            continue

        if not patched_file.exists():
            item["schema_valid"] = False
            item["schema_error"] = f"patched_file_missing:{patched_file}"
            reports.append(
                {
                    "flow_uuid": flow_uuid,
                    "patched_file": str(patched_file),
                    "status": "error",
                    "schema_valid": False,
                    "schema_error": item["schema_error"],
                }
            )
            invalid_count += 1
            updated_rows.append(item)
            continue

        validated_count += 1
        wrapper = _flow_wrapper(_read_json(patched_file))
        schema_valid, schema_error = _validate_ilcd_flow_schema(wrapper)
        item["schema_valid"] = schema_valid
        item["schema_error"] = schema_error
        if schema_valid:
            valid_count += 1
            _write_json(schema_valid_dir / patched_file.name, wrapper)
            reports.append(
                {
                    "flow_uuid": flow_uuid,
                    "patched_file": str(patched_file),
                    "status": "ok",
                    "schema_valid": True,
                }
            )
        else:
            invalid_count += 1
            reports.append(
                {
                    "flow_uuid": flow_uuid,
                    "patched_file": str(patched_file),
                    "status": "invalid",
                    "schema_valid": False,
                    "schema_error": schema_error,
                }
            )
        updated_rows.append(item)

    _write_jsonl(manifest_path, updated_rows)
    _write_jsonl(out_dir / "schema_validation.jsonl", reports)
    summary = {
        "row_count": len(rows),
        "validated_count": validated_count,
        "schema_valid_count": valid_count,
        "schema_invalid_count": invalid_count,
        "schema_valid_flows_dir": str(schema_valid_dir),
    }
    _write_json(out_dir / "schema_validation_summary.json", summary)
    return summary


def _extract_record_version_from_select_record(record: Mapping[str, Any]) -> str:
    # Prefer explicit DB columns when available; fallback to embedded JSON.
    for key in ("version", "data_set_version", "dataset_version"):
        value = _coerce_text(record.get(key))
        if value:
            return value
    for key in ("json_ordered", "json"):
        payload = record.get(key)
        if isinstance(payload, Mapping):
            if isinstance(payload.get("flowDataSet"), Mapping):
                return _flow_version(payload["flowDataSet"])
    return ""


def _base_semantic_hash_for_row(
    row: Mapping[str, Any],
    *,
    source_hash_cache: dict[str, str],
) -> str:
    direct = _coerce_text(row.get("before_sha256_no_version"))
    if direct:
        return direct
    source_file = _coerce_text(row.get("source_file"))
    if not source_file:
        return ""
    if source_file in source_hash_cache:
        return source_hash_cache[source_file]
    path = Path(source_file)
    if not path.exists():
        source_hash_cache[source_file] = ""
        return ""
    try:
        source_hash_cache[source_file] = _sha256_flow_without_version(_read_json(path))
    except Exception:
        source_hash_cache[source_file] = ""
    return source_hash_cache[source_file]


def _latest_semantically_matches_base(
    latest_doc: Mapping[str, Any] | None,
    row: Mapping[str, Any],
    *,
    source_hash_cache: dict[str, str],
) -> bool:
    if not isinstance(latest_doc, Mapping):
        return False
    base_hash = _base_semantic_hash_for_row(row, source_hash_cache=source_hash_cache)
    if not base_hash:
        return False
    return _sha256_flow_without_version(latest_doc) == base_hash


def _publish_patched_flows(
    manifest_path: Path,
    out_dir: Path,
    *,
    mode: str = "dry-run",
    require_latest_match: bool = True,
    only_changed: bool = True,
    version_conflict_retries: int = 20,
) -> dict[str, Any]:
    manifest_rows = _load_jsonl(manifest_path)
    if not manifest_rows:
        out_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "row_count": 0,
            "mode": mode,
            "version_conflict_retries": max(0, int(version_conflict_retries)),
            "inserted_count": 0,
            "dry_run_count": 0,
            "idempotent_skip_count": 0,
            "schema_gate_skipped_count": 0,
            "auto_retarget_count": 0,
            "retry_insert_count": 0,
            "retry_exhausted_count": 0,
            "conflict_count": 0,
            "error_count": 0,
            "skipped": True,
            "reason": f"empty_manifest:{manifest_path}",
        }
        _write_jsonl(out_dir / "publish_results.jsonl", [])
        _write_json(out_dir / "publish_summary.json", summary)
        return summary

    mcp = McpCrudFacade.create()
    results: list[dict[str, Any]] = []
    source_hash_cache: dict[str, str] = {}
    inserted = 0
    skipped = 0
    idempotent_skip_count = 0
    schema_gate_skipped = 0
    auto_retarget_count = 0
    retry_insert_count = 0
    retry_exhausted_count = 0
    conflicts = 0
    errors = 0
    try:
        for row in manifest_rows:
            if only_changed and not bool(row.get("changed")):
                continue
            if row.get("schema_valid") is False:
                results.append(
                    {
                        "flow_uuid": _coerce_text(row.get("flow_uuid")),
                        "base_version": _coerce_text(row.get("base_version")),
                        "mode": mode,
                        "status": "skipped",
                        "reason": "schema_invalid",
                    }
                )
                schema_gate_skipped += 1
                continue
            patched_file = Path(_coerce_text(row.get("patched_file")))
            if not patched_file.exists():
                results.append(
                    {
                        "flow_uuid": _coerce_text(row.get("flow_uuid")),
                        "status": "error",
                        "reason": f"patched_file_missing:{patched_file}",
                    }
                )
                errors += 1
                continue

            wrapper = _flow_wrapper(_read_json(patched_file))
            flow_ds = wrapper["flowDataSet"]
            flow_uuid = _flow_uuid(flow_ds)
            base_version = _coerce_text(row.get("base_version"))
            patched_version = _flow_version(flow_ds)

            latest_doc = mcp.select_flow(flow_uuid)
            latest_version = _flow_version(_flow_root(latest_doc)) if latest_doc else ""
            retargeted = False
            retarget_reason = ""

            if require_latest_match and base_version and latest_version and base_version != latest_version:
                if _latest_semantically_matches_base(latest_doc, row, source_hash_cache=source_hash_cache):
                    patched_version = _bump_ilcd_version(latest_version)
                    _set_flow_version(flow_ds, patched_version)
                    retargeted = True
                    retarget_reason = "base_version_mismatch_semantically_equal_auto_retarget"
                    auto_retarget_count += 1
                else:
                    results.append(
                        {
                            "flow_uuid": flow_uuid,
                            "base_version": base_version,
                            "latest_version": latest_version,
                            "status": "conflict",
                            "reason": "base_version_mismatch",
                        }
                    )
                    conflicts += 1
                    continue

            expected_version = _bump_ilcd_version(latest_version or base_version or patched_version or "01.01.000")
            if not patched_version:
                results.append(
                    {
                        "flow_uuid": flow_uuid,
                        "base_version": base_version,
                        "latest_version_checked": latest_version,
                        "mode": mode,
                        "status": "error",
                        "reason": "patched_version_missing",
                    }
                )
                errors += 1
                continue
            if expected_version and patched_version != expected_version:
                results.append(
                    {
                        "flow_uuid": flow_uuid,
                        "base_version": base_version,
                        "latest_version_checked": latest_version,
                        "patched_version": patched_version,
                        "expected_version": expected_version,
                        "mode": mode,
                        "status": "error",
                        "reason": "patched_version_not_latest_plus_one",
                    }
                )
                errors += 1
                continue

            after_hash = _sha256_json(wrapper)

            result_row = {
                "flow_uuid": flow_uuid,
                "base_version": base_version,
                "latest_version_checked": latest_version,
                "new_version": patched_version or expected_version,
                "mode": mode,
                "status": "dry-run",
                "after_sha256": after_hash,
            }
            if retargeted:
                result_row["version_retargeted"] = True
                result_row["retarget_reason"] = retarget_reason

            if mode == "insert":
                try:
                    insert_result = mcp.insert_flow(wrapper)
                    result_row["status"] = "inserted"
                    result_row["insert_result"] = insert_result
                    inserted += 1
                except Exception as exc:  # noqa: BLE001
                    err_text = str(exc)
                    if _is_version_conflict_error(err_text):
                        latest_after = mcp.select_flow(flow_uuid)
                        latest_after_version = _flow_version(_flow_root(latest_after)) if latest_after else ""
                        result_row["latest_version_after_conflict"] = latest_after_version
                        if (
                            isinstance(latest_after, Mapping)
                            and _sha256_flow_without_version(latest_after) == _sha256_flow_without_version(wrapper)
                        ):
                            result_row["status"] = "skipped"
                            result_row["reason"] = "already_published_equivalent"
                            idempotent_skip_count += 1
                        else:
                            can_retry = False
                            retry_basis = ""
                            retry_seed = latest_after_version or patched_version
                            if _latest_semantically_matches_base(latest_after, row, source_hash_cache=source_hash_cache):
                                can_retry = True
                                retry_basis = "base_semantic_match"
                            elif latest_after is None:
                                can_retry = True
                                retry_basis = "latest_unreadable"
                                retry_seed = patched_version
                            else:
                                base_hash = _base_semantic_hash_for_row(row, source_hash_cache=source_hash_cache)
                                if not base_hash:
                                    can_retry = True
                                    retry_basis = "base_semantic_unknown"
                            max_retries = max(0, int(version_conflict_retries))
                            if can_retry and max_retries > 0:
                                result_row["conflict_retry_basis"] = retry_basis
                                retry_version = _bump_ilcd_version(retry_seed or patched_version or "01.01.000")
                                retry_success = False
                                for attempt in range(1, max_retries + 1):
                                    _set_flow_version(flow_ds, retry_version)
                                    result_row["retry_version"] = retry_version
                                    try:
                                        insert_result = mcp.insert_flow(wrapper)
                                        result_row["status"] = "inserted"
                                        result_row["insert_result"] = insert_result
                                        result_row["retried_after_conflict"] = True
                                        result_row["retry_attempts"] = attempt
                                        result_row["new_version"] = retry_version
                                        retry_insert_count += 1
                                        inserted += 1
                                        retry_success = True
                                        break
                                    except Exception as exc_retry:  # noqa: BLE001
                                        retry_error = str(exc_retry)
                                        if _is_version_conflict_error(retry_error):
                                            retry_version = _bump_ilcd_version(retry_version)
                                            continue
                                        result_row["status"] = "error"
                                        result_row["reason"] = retry_error
                                        errors += 1
                                        retry_success = True
                                        break
                                if not retry_success:
                                    result_row["status"] = "conflict"
                                    result_row["reason"] = "version_conflict_retry_exhausted"
                                    result_row["retry_attempts"] = max_retries
                                    retry_exhausted_count += 1
                                    conflicts += 1
                            elif can_retry and max_retries <= 0:
                                result_row["status"] = "conflict"
                                result_row["reason"] = "version_conflict_retry_disabled"
                                conflicts += 1
                            else:
                                result_row["status"] = "conflict"
                                result_row["reason"] = "insert_version_conflict_with_base_drift"
                                conflicts += 1
                    else:
                        result_row["status"] = "error"
                        result_row["reason"] = err_text
                        errors += 1
            else:
                skipped += 1

            results.append(result_row)

        out_dir.mkdir(parents=True, exist_ok=True)
        _write_jsonl(out_dir / "publish_results.jsonl", results)
        summary = {
            "row_count": len(results),
            "mode": mode,
            "version_conflict_retries": max(0, int(version_conflict_retries)),
            "inserted_count": inserted,
            "dry_run_count": skipped,
            "idempotent_skip_count": idempotent_skip_count,
            "schema_gate_skipped_count": schema_gate_skipped,
            "auto_retarget_count": auto_retarget_count,
            "retry_insert_count": retry_insert_count,
            "retry_exhausted_count": retry_exhausted_count,
            "conflict_count": conflicts,
            "error_count": errors,
        }
        _write_json(out_dir / "publish_summary.json", summary)
        return summary
    finally:
        mcp.close()


def _fetch_flows(uuid_list_path: Path, out_dir: Path, *, limit: int | None = None) -> dict[str, Any]:
    rows = load_uuid_list(uuid_list_path)
    if limit is not None:
        rows = rows[:limit]
    if not rows:
        raise RuntimeError(f"No UUIDs parsed from {uuid_list_path}")

    cache_flows = out_dir / "cache" / "flows"
    cache_flows.mkdir(parents=True, exist_ok=True)
    fetch_log: list[dict[str, Any]] = []

    mcp = McpCrudFacade.create()
    ok = 0
    miss = 0
    err = 0
    try:
        for item in rows:
            flow_id = item["id"]
            version = item.get("version")
            try:
                doc = mcp.select_flow(flow_id, version=version or None)
                if not doc:
                    fetch_log.append({"id": flow_id, "requested_version": version or "", "status": "not_found"})
                    miss += 1
                    continue
                flow_ds = _flow_root(doc)
                actual_uuid = _flow_uuid(flow_ds) or flow_id
                actual_version = _flow_version(flow_ds) or (version or "")
                filename = f"{actual_uuid}_{actual_version or 'unknown'}.json"
                out_file = cache_flows / filename
                _write_json(out_file, doc)
                fetch_log.append(
                    {
                        "id": flow_id,
                        "requested_version": version or "",
                        "actual_uuid": actual_uuid,
                        "actual_version": actual_version,
                        "file": str(out_file),
                        "status": "ok",
                    }
                )
                ok += 1
            except Exception as exc:  # noqa: BLE001
                fetch_log.append(
                    {
                        "id": flow_id,
                        "requested_version": version or "",
                        "status": "error",
                        "reason": str(exc),
                    }
                )
                err += 1

        _write_jsonl(out_dir / "fetch" / "fetch_log.jsonl", fetch_log)
        summary = {
            "requested_count": len(rows),
            "ok_count": ok,
            "not_found_count": miss,
            "error_count": err,
            "cache_flows_dir": str(cache_flows),
        }
        _write_json(out_dir / "fetch" / "fetch_summary.json", summary)
        return summary
    finally:
        mcp.close()


def _request_from_existing_flow(flow_doc: Mapping[str, Any], overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    flow_ds = _flow_root(flow_doc)
    class_entries = _classification_classes(flow_ds)
    leaf = _classification_leaf(flow_ds)
    name = _name_node(flow_ds) if isinstance(_name_node(flow_ds), Mapping) else {}
    if not isinstance(name, Mapping):
        name = {}
    comments = _deep_get(flow_ds, ("flowInformation", "dataSetInformation", "common:generalComment"))
    synonyms = _deep_get(flow_ds, ("flowInformation", "dataSetInformation", "common:synonyms"))
    prop, _ = _pick_reference_flow_property(flow_ds)
    pref = _flow_property_ref(prop or {})

    def _split_synonyms(value: Any, lang: str) -> list[str]:
        text = _lang_text(value, lang)
        if not text:
            return []
        raw = text.replace("；", ";")
        pieces = [p.strip() for chunk in raw.split(";") for p in chunk.split(",")]
        return [p for p in pieces if p]

    request = {
        "class_id": leaf["class_id"] or "unknown",
        "classification": class_entries,
        "base_name_en": _lang_text(name.get("baseName"), "en"),
        "base_name_zh": _lang_text(name.get("baseName"), "zh"),
        "treatment_en": _lang_text(name.get("treatmentStandardsRoutes"), "en"),
        "treatment_zh": _lang_text(name.get("treatmentStandardsRoutes"), "zh"),
        "mix_en": _lang_text(name.get("mixAndLocationTypes"), "en"),
        "mix_zh": _lang_text(name.get("mixAndLocationTypes"), "zh"),
        "comment_en": _lang_text(comments, "en"),
        "comment_zh": _lang_text(comments, "zh"),
        "synonyms_en": _split_synonyms(synonyms, "en"),
        "synonyms_zh": _split_synonyms(synonyms, "zh"),
        "flow_type": _flow_type(flow_ds) or "Product flow",
        "flow_uuid": _flow_uuid(flow_ds),
        "version": _flow_version(flow_ds) or "01.01.000",
        "flow_property_uuid": pref["uuid"],
        "flow_property_version": pref["version"] or "03.00.003",
        "flow_property_name_en": pref["short_name"] or "Mass",
    }
    if overrides:
        for key, value in overrides.items():
            if key in request:
                request[key] = value
    return request


def _regenerate_product_flow(
    flow_file: Path,
    out_file: Path,
    *,
    overrides_file: Path | None = None,
    allow_validation_fallback: bool = False,
) -> dict[str, Any]:
    _ensure_process_builder_on_syspath()
    from tiangong_lca_spec.product_flow_creation import ProductFlowCreateRequest, ProductFlowCreationService

    flow_doc = _read_json(flow_file)
    overrides = _read_json(overrides_file) if overrides_file else None
    if overrides is not None and not isinstance(overrides, Mapping):
        raise RuntimeError(f"Overrides must be a JSON object: {overrides_file}")
    request_payload = _request_from_existing_flow(flow_doc, overrides=overrides if isinstance(overrides, Mapping) else None)
    request = ProductFlowCreateRequest(**request_payload)
    service = ProductFlowCreationService()
    result = service.build(request, allow_validation_fallback=allow_validation_fallback)
    _write_json(out_file, result.payload)
    summary = {
        "flow_uuid": result.flow_uuid,
        "version": result.version,
        "output_file": str(out_file),
        "reused_service": "process-automated-builder/tiangong_lca_spec/product_flow_creation/service.py",
    }
    return summary


def _run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    fetch_summary = _fetch_flows(Path(args.uuid_list).resolve(), run_dir, limit=args.limit)
    review_summary = _run_lifecycleinventory_review_flow(
        flows_dir=run_dir / "cache" / "flows",
        out_dir=run_dir / "review",
        run_root=run_dir,
        run_id=getattr(args, "review_run_id", None),
        start_ts=getattr(args, "review_start_ts", None),
        end_ts=getattr(args, "review_end_ts", None),
        logic_version=getattr(args, "review_logic_version", None),
        enable_llm=(getattr(args, "review_enable_llm", None) is True),
        disable_llm=(getattr(args, "review_enable_llm", None) is False),
        llm_model=getattr(args, "review_llm_model", None),
        llm_max_flows=getattr(args, "review_llm_max_flows", None),
        llm_batch_size=getattr(args, "review_llm_batch_size", None),
        with_reference_context=bool(args.with_reference_context),
        similarity_threshold=args.similarity_threshold,
    )
    llm_remediate_summary = _llm_remediate_findings(
        run_dir / "cache" / "flows",
        run_dir / "review" / "findings.jsonl",
        run_dir / "fix",
        copy_unchanged=bool(args.copy_unchanged or args.publish_include_unchanged),
        llm_model=getattr(args, "remediate_llm_model", None),
        llm_base_url=getattr(args, "remediate_llm_base_url", None),
        llm_api_key=getattr(args, "remediate_llm_api_key", None),
        llm_timeout_sec=getattr(args, "remediate_llm_timeout_sec", 120),
    )
    bump_summary = _bump_versions_if_needed(
        run_dir / "fix" / "patch_manifest.jsonl",
        run_dir / "fix",
        include_unchanged=bool(args.copy_unchanged or args.publish_include_unchanged),
    )
    validate_schema_summary = _validate_schema_outputs(
        run_dir / "fix" / "patch_manifest.jsonl",
        run_dir / "fix",
        include_unchanged=bool(args.copy_unchanged or args.publish_include_unchanged),
    )
    schema_valid_flows_dir = Path(
        _coerce_text(validate_schema_summary.get("schema_valid_flows_dir")) or (run_dir / "fix" / "schema_valid_flows")
    )
    patched_files = _iter_flow_files(schema_valid_flows_dir)
    if patched_files:
        validate_summary = _run_lifecycleinventory_review_flow(
            flows_dir=schema_valid_flows_dir,
            out_dir=run_dir / "validate",
            run_root=run_dir,
            run_id=(f"{getattr(args, 'review_run_id')}-validate" if getattr(args, "review_run_id", None) else None),
            start_ts=getattr(args, "review_start_ts", None),
            end_ts=getattr(args, "review_end_ts", None),
            logic_version=getattr(args, "review_logic_version", None),
            enable_llm=(getattr(args, "review_enable_llm", None) is True),
            disable_llm=(getattr(args, "review_enable_llm", None) is False),
            llm_model=getattr(args, "review_llm_model", None),
            llm_max_flows=getattr(args, "review_llm_max_flows", None),
            llm_batch_size=getattr(args, "review_llm_batch_size", None),
            with_reference_context=bool(args.with_reference_context),
            similarity_threshold=args.similarity_threshold,
        )
    else:
        validate_summary = {
            "flow_count": 0,
            "finding_count": 0,
            "similarity_pair_count": 0,
            "with_reference_context": bool(args.with_reference_context),
            "skipped": True,
            "reason": f"no_schema_valid_flows:{schema_valid_flows_dir}",
        }
        _write_json(run_dir / "validate" / "flow_review_summary.json", validate_summary)

    publish_summary: dict[str, Any] | None = None
    if args.publish_mode != "none":
        publish_summary = _publish_patched_flows(
            run_dir / "fix" / "patch_manifest.jsonl",
            run_dir / "publish",
            mode=args.publish_mode,
            require_latest_match=not args.skip_base_check,
            only_changed=not args.publish_include_unchanged,
            version_conflict_retries=args.publish_version_conflict_retries,
        )

    pipeline_summary = {
        "run_dir": str(run_dir),
        "fetch": fetch_summary,
        "review": review_summary,
        "llm_remediate": llm_remediate_summary,
        "bump_version_if_needed": bump_summary,
        "validate_schema": validate_schema_summary,
        "validate": validate_summary,
        "publish": publish_summary,
    }
    _write_json(run_dir / "pipeline_summary.json", pipeline_summary)
    return pipeline_summary


def _print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Flow remediation + append-only publish pipeline (initial skill version)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_fetch = sub.add_parser("fetch", help="Fetch flow JSONs via MCP CRUD from a UUID list.")
    p_fetch.add_argument("--uuid-list", required=True, help="Path to UUID list JSON/JSONL/TXT.")
    p_fetch.add_argument("--run-dir", required=True, help="Run directory for cache and outputs.")
    p_fetch.add_argument("--limit", type=int, help="Optional limit for dry runs.")

    p_review = sub.add_parser("review", help="Run flow review via lifecycleinventory-review --profile flow on local flow JSON cache.")
    p_review.add_argument("--flows-dir", help="Directory of flow JSON files. Defaults to <run-dir>/cache/flows.")
    p_review.add_argument("--run-dir", help="Run directory. Used when --flows-dir omitted.")
    p_review.add_argument("--out-dir", help="Output dir. Defaults to <run-dir>/review.")
    p_review.add_argument(
        "--with-reference-context",
        action="store_true",
        help="Enable flowproperty/unitgroup reference-context enrichment in lifecycleinventory-review (uses local process-automated-builder registry).",
    )
    p_review.add_argument("--similarity-threshold", type=float, default=0.92)
    p_review.add_argument("--max-pairs-per-group", type=int, default=20000)
    p_review.add_argument("--review-run-id", help="Passed through to lifecycleinventory-review flow profile for reporting.")
    p_review.add_argument("--review-start-ts", help="Passed through to lifecycleinventory-review flow profile.")
    p_review.add_argument("--review-end-ts", help="Passed through to lifecycleinventory-review flow profile.")
    p_review.add_argument("--review-logic-version", help="Passed through to lifecycleinventory-review flow profile.")
    p_review.add_argument(
        "--review-enable-llm",
        dest="review_enable_llm",
        action="store_true",
        default=None,
        help="Force enable LLM semantic review in lifecycleinventory-review flow profile.",
    )
    p_review.add_argument(
        "--review-disable-llm",
        dest="review_enable_llm",
        action="store_false",
        help="Force disable LLM semantic review in lifecycleinventory-review flow profile.",
    )
    p_review.add_argument("--review-llm-model", help="LLM model passed to lifecycleinventory-review flow profile.")
    p_review.add_argument("--review-llm-max-flows", type=int, help="Max flows for LLM review in lifecycleinventory-review flow profile.")
    p_review.add_argument("--review-llm-batch-size", type=int, help="LLM batch size in lifecycleinventory-review flow profile.")

    def _add_remediate_args(parser_obj: argparse.ArgumentParser) -> None:
        parser_obj.add_argument("--run-dir", help="Run directory. Defaults inputs/outputs under it.")
        parser_obj.add_argument("--flows-dir", help="Directory of flow JSON files. Defaults to <run-dir>/cache/flows.")
        parser_obj.add_argument("--findings", help="Path to findings.jsonl. Defaults to <run-dir>/review/findings.jsonl.")
        parser_obj.add_argument("--out-dir", help="Output dir. Defaults to <run-dir>/fix.")
        parser_obj.add_argument(
            "--copy-unchanged",
            action="store_true",
            help="Copy unchanged flows into patched_flows so they can optionally pass later stages.",
        )
        parser_obj.add_argument("--remediate-llm-model", help="LLM model for remediation stage.")
        parser_obj.add_argument("--remediate-llm-base-url", help="OpenAI-compatible base URL for remediation stage.")
        parser_obj.add_argument("--remediate-llm-api-key", help="Explicit API key for remediation stage.")
        parser_obj.add_argument("--remediate-llm-timeout-sec", type=int, default=120, help="LLM timeout per issue.")

    p_llm_fix = sub.add_parser(
        "llm-remediate",
        help="Per-finding LLM remediation with structured output and patch manifest generation.",
    )
    _add_remediate_args(p_llm_fix)

    p_bump = sub.add_parser(
        "bump-version-if-needed",
        help="Ensure patched flow versions equal base_version+1 (no version jumps).",
    )
    p_bump.add_argument("--run-dir", help="Run directory. Defaults manifest/out under <run-dir>/fix.")
    p_bump.add_argument("--manifest", help="Patch manifest JSONL. Defaults to <run-dir>/fix/patch_manifest.jsonl.")
    p_bump.add_argument("--out-dir", help="Output dir. Defaults to <run-dir>/fix.")
    p_bump.add_argument("--include-unchanged", action="store_true", help="Also process unchanged copied flows.")

    p_schema = sub.add_parser(
        "validate-schema",
        help="Validate patched flows against ILCD FlowDataSet schema and update manifest schema flags.",
    )
    p_schema.add_argument("--run-dir", help="Run directory. Defaults manifest/out under <run-dir>/fix.")
    p_schema.add_argument("--manifest", help="Patch manifest JSONL. Defaults to <run-dir>/fix/patch_manifest.jsonl.")
    p_schema.add_argument("--out-dir", help="Output dir. Defaults to <run-dir>/fix.")
    p_schema.add_argument("--include-unchanged", action="store_true", help="Validate unchanged copied flows too.")

    p_validate = sub.add_parser("validate", help="Re-run lifecycleinventory-review --profile flow on schema-valid patched flows.")
    p_validate.add_argument(
        "--run-dir",
        help="Run directory. Defaults to <run-dir>/fix/schema_valid_flows when present, otherwise <run-dir>/fix/patched_flows.",
    )
    p_validate.add_argument("--flows-dir", help="Directory of patched flow JSON files.")
    p_validate.add_argument("--out-dir", help="Output dir. Defaults to <run-dir>/validate.")
    p_validate.add_argument(
        "--with-reference-context",
        action="store_true",
        help="Enable flowproperty/unitgroup reference-context enrichment in lifecycleinventory-review validate stage (uses local registry).",
    )
    p_validate.add_argument("--similarity-threshold", type=float, default=0.92)
    p_validate.add_argument("--max-pairs-per-group", type=int, default=20000)
    p_validate.add_argument("--review-run-id", help="Passed through to lifecycleinventory-review flow profile for reporting.")
    p_validate.add_argument("--review-start-ts", help="Passed through to lifecycleinventory-review flow profile.")
    p_validate.add_argument("--review-end-ts", help="Passed through to lifecycleinventory-review flow profile.")
    p_validate.add_argument("--review-logic-version", help="Passed through to lifecycleinventory-review flow profile.")
    p_validate.add_argument(
        "--review-enable-llm",
        dest="review_enable_llm",
        action="store_true",
        default=None,
        help="Force enable LLM semantic review in lifecycleinventory-review flow profile.",
    )
    p_validate.add_argument(
        "--review-disable-llm",
        dest="review_enable_llm",
        action="store_false",
        help="Force disable LLM semantic review in lifecycleinventory-review flow profile.",
    )
    p_validate.add_argument("--review-llm-model", help="LLM model passed to lifecycleinventory-review flow profile.")
    p_validate.add_argument("--review-llm-max-flows", type=int, help="Max flows for LLM review in lifecycleinventory-review flow profile.")
    p_validate.add_argument("--review-llm-batch-size", type=int, help="LLM batch size in lifecycleinventory-review flow profile.")

    p_publish = sub.add_parser("publish", help="Append-only publish patched flows by MCP CRUD insert.")
    p_publish.add_argument("--run-dir", help="Run directory. Uses <run-dir>/fix/patch_manifest.jsonl and outputs to <run-dir>/publish.")
    p_publish.add_argument("--manifest", help="Patch manifest JSONL. Defaults to <run-dir>/fix/patch_manifest.jsonl.")
    p_publish.add_argument("--out-dir", help="Output dir. Defaults to <run-dir>/publish.")
    p_publish.add_argument("--mode", choices=["dry-run", "insert"], default="dry-run")
    p_publish.add_argument("--skip-base-check", action="store_true", help="Do not require latest DB version to match base_version.")
    p_publish.add_argument("--include-unchanged", action="store_true", help="Also publish unchanged copied flows (normally skipped).")
    p_publish.add_argument(
        "--version-conflict-retries",
        type=int,
        default=20,
        help="When insert reports version conflict, retry with +1 versions up to this many times (for blind/private version gaps).",
    )

    p_pipeline = sub.add_parser(
        "pipeline",
        help="Run fetch -> review -> llm-remediate -> bump-version-if-needed -> validate-schema -> validate -> optional publish.",
    )
    p_pipeline.add_argument("--uuid-list", required=True)
    p_pipeline.add_argument("--run-dir", required=True)
    p_pipeline.add_argument("--limit", type=int)
    p_pipeline.add_argument(
        "--with-reference-context",
        action="store_true",
        help="Enable lifecycleinventory-review flow reference-context enrichment during review/validate (uses local registry).",
    )
    p_pipeline.add_argument("--similarity-threshold", type=float, default=0.92)
    p_pipeline.add_argument("--max-pairs-per-group", type=int, default=20000)
    p_pipeline.add_argument("--review-run-id", help="Passed through to lifecycleinventory-review flow profile for reporting.")
    p_pipeline.add_argument("--review-start-ts", help="Passed through to lifecycleinventory-review flow profile.")
    p_pipeline.add_argument("--review-end-ts", help="Passed through to lifecycleinventory-review flow profile.")
    p_pipeline.add_argument("--review-logic-version", help="Passed through to lifecycleinventory-review flow profile.")
    p_pipeline.add_argument(
        "--review-enable-llm",
        dest="review_enable_llm",
        action="store_true",
        default=None,
        help="Force enable LLM semantic review in lifecycleinventory-review flow profile.",
    )
    p_pipeline.add_argument(
        "--review-disable-llm",
        dest="review_enable_llm",
        action="store_false",
        help="Force disable LLM semantic review in lifecycleinventory-review flow profile.",
    )
    p_pipeline.add_argument("--review-llm-model", help="LLM model passed to lifecycleinventory-review flow profile.")
    p_pipeline.add_argument("--review-llm-max-flows", type=int, help="Max flows for LLM review in lifecycleinventory-review flow profile.")
    p_pipeline.add_argument("--review-llm-batch-size", type=int, help="LLM batch size in lifecycleinventory-review flow profile.")
    p_pipeline.add_argument("--remediate-llm-model", help="LLM model for remediation stage.")
    p_pipeline.add_argument("--remediate-llm-base-url", help="OpenAI-compatible base URL for remediation stage.")
    p_pipeline.add_argument("--remediate-llm-api-key", help="Explicit API key for remediation stage.")
    p_pipeline.add_argument("--remediate-llm-timeout-sec", type=int, default=120, help="LLM timeout per issue.")
    p_pipeline.add_argument("--copy-unchanged", action="store_true")
    p_pipeline.add_argument("--publish-mode", choices=["none", "dry-run", "insert"], default="none")
    p_pipeline.add_argument("--skip-base-check", action="store_true")
    p_pipeline.add_argument("--publish-include-unchanged", action="store_true")
    p_pipeline.add_argument(
        "--publish-version-conflict-retries",
        type=int,
        default=20,
        help="Publish-stage +1 retry attempts after version-conflict insert errors.",
    )

    p_regen = sub.add_parser(
        "regen-product-flow",
        help="Rebuild a product flow using process-automated-builder ProductFlowCreationService (for classification/name changes).",
    )
    p_regen.add_argument("--flow-file", required=True)
    p_regen.add_argument("--out-file", required=True)
    p_regen.add_argument("--overrides-file", help="JSON object overriding ProductFlowCreateRequest fields.")
    p_regen.add_argument("--allow-validation-fallback", action="store_true")

    return parser


def _require_run_dir(value: str | None, parser: argparse.ArgumentParser, sub_name: str) -> Path:
    if not value:
        parser.error(f"{sub_name} requires --run-dir when explicit paths are not provided")
    return Path(value).resolve()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "fetch":
            summary = _fetch_flows(Path(args.uuid_list).resolve(), Path(args.run_dir).resolve(), limit=args.limit)
            _print_json(summary)
            return 0

        if args.command == "review":
            if args.flows_dir:
                flows_dir = Path(args.flows_dir).resolve()
                if args.out_dir:
                    out_dir = Path(args.out_dir).resolve()
                else:
                    run_dir = _require_run_dir(args.run_dir, parser, "review")
                    out_dir = run_dir / "review"
                run_root = Path(args.run_dir).resolve() if args.run_dir else None
            else:
                run_dir = _require_run_dir(args.run_dir, parser, "review")
                flows_dir = run_dir / "cache" / "flows"
                out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir / "review")
                run_root = run_dir
            summary = _run_lifecycleinventory_review_flow(
                flows_dir=flows_dir,
                out_dir=out_dir,
                run_root=run_root,
                run_id=args.review_run_id,
                start_ts=args.review_start_ts,
                end_ts=args.review_end_ts,
                logic_version=args.review_logic_version,
                enable_llm=(args.review_enable_llm is True),
                disable_llm=(args.review_enable_llm is False),
                llm_model=args.review_llm_model,
                llm_max_flows=args.review_llm_max_flows,
                llm_batch_size=args.review_llm_batch_size,
                with_reference_context=bool(args.with_reference_context),
                similarity_threshold=args.similarity_threshold,
            )
            _print_json(summary)
            return 0

        if args.command == "llm-remediate":
            run_dir = Path(args.run_dir).resolve() if args.run_dir else None
            if args.flows_dir:
                flows_dir = Path(args.flows_dir).resolve()
            else:
                if run_dir is None:
                    parser.error(f"{args.command} requires --run-dir or --flows-dir")
                flows_dir = run_dir / "cache" / "flows"
            if args.findings:
                findings_path = Path(args.findings).resolve()
            else:
                if run_dir is None:
                    parser.error(f"{args.command} requires --run-dir or --findings")
                findings_path = run_dir / "review" / "findings.jsonl"
            if args.out_dir:
                out_dir = Path(args.out_dir).resolve()
            else:
                if run_dir is None:
                    parser.error(f"{args.command} requires --run-dir or --out-dir")
                out_dir = run_dir / "fix"
            summary = _llm_remediate_findings(
                flows_dir,
                findings_path,
                out_dir,
                copy_unchanged=bool(args.copy_unchanged),
                llm_model=getattr(args, "remediate_llm_model", None),
                llm_base_url=getattr(args, "remediate_llm_base_url", None),
                llm_api_key=getattr(args, "remediate_llm_api_key", None),
                llm_timeout_sec=getattr(args, "remediate_llm_timeout_sec", 120),
            )
            _print_json(summary)
            return 0

        if args.command == "bump-version-if-needed":
            run_dir = Path(args.run_dir).resolve() if args.run_dir else None
            manifest_path = (
                Path(args.manifest).resolve()
                if args.manifest
                else (run_dir / "fix" / "patch_manifest.jsonl" if run_dir else None)
            )
            out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir / "fix" if run_dir else None)
            if manifest_path is None or out_dir is None:
                parser.error("bump-version-if-needed requires --run-dir or both --manifest and --out-dir")
            summary = _bump_versions_if_needed(
                manifest_path,
                out_dir,
                include_unchanged=bool(args.include_unchanged),
            )
            _print_json(summary)
            return 0

        if args.command == "validate-schema":
            run_dir = Path(args.run_dir).resolve() if args.run_dir else None
            manifest_path = (
                Path(args.manifest).resolve()
                if args.manifest
                else (run_dir / "fix" / "patch_manifest.jsonl" if run_dir else None)
            )
            out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir / "fix" if run_dir else None)
            if manifest_path is None or out_dir is None:
                parser.error("validate-schema requires --run-dir or both --manifest and --out-dir")
            summary = _validate_schema_outputs(
                manifest_path,
                out_dir,
                include_unchanged=bool(args.include_unchanged),
            )
            _print_json(summary)
            return 0

        if args.command == "validate":
            if args.flows_dir:
                flows_dir = Path(args.flows_dir).resolve()
                if args.out_dir:
                    out_dir = Path(args.out_dir).resolve()
                else:
                    run_dir = _require_run_dir(args.run_dir, parser, "validate")
                    out_dir = run_dir / "validate"
                run_root = Path(args.run_dir).resolve() if args.run_dir else None
            else:
                run_dir = _require_run_dir(args.run_dir, parser, "validate")
                schema_dir = run_dir / "fix" / "schema_valid_flows"
                flows_dir = schema_dir if schema_dir.exists() else (run_dir / "fix" / "patched_flows")
                out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir / "validate")
                run_root = run_dir
            if not _iter_flow_files(flows_dir):
                summary = {
                    "flow_count": 0,
                    "finding_count": 0,
                    "similarity_pair_count": 0,
                    "with_reference_context": bool(args.with_reference_context),
                    "skipped": True,
                    "reason": f"no_flow_files:{flows_dir}",
                }
                _write_json(out_dir / "flow_review_summary.json", summary)
            else:
                summary = _run_lifecycleinventory_review_flow(
                    flows_dir=flows_dir,
                    out_dir=out_dir,
                    run_root=run_root,
                    run_id=(f"{args.review_run_id}-validate" if args.review_run_id else None),
                    start_ts=args.review_start_ts,
                    end_ts=args.review_end_ts,
                    logic_version=args.review_logic_version,
                    enable_llm=(args.review_enable_llm is True),
                    disable_llm=(args.review_enable_llm is False),
                    llm_model=args.review_llm_model,
                    llm_max_flows=args.review_llm_max_flows,
                    llm_batch_size=args.review_llm_batch_size,
                    with_reference_context=bool(args.with_reference_context),
                    similarity_threshold=args.similarity_threshold,
                )
            _print_json(summary)
            return 0

        if args.command == "publish":
            run_dir = Path(args.run_dir).resolve() if args.run_dir else None
            manifest_path = (
                Path(args.manifest).resolve()
                if args.manifest
                else (run_dir / "fix" / "patch_manifest.jsonl" if run_dir else None)
            )
            out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir / "publish" if run_dir else None)
            if manifest_path is None or out_dir is None:
                parser.error("publish requires --run-dir or both --manifest and --out-dir")
            summary = _publish_patched_flows(
                manifest_path,
                out_dir,
                mode=args.mode,
                require_latest_match=not args.skip_base_check,
                only_changed=not args.include_unchanged,
                version_conflict_retries=args.version_conflict_retries,
            )
            _print_json(summary)
            return 0

        if args.command == "pipeline":
            summary = _run_pipeline(args)
            _print_json(summary)
            return 0

        if args.command == "regen-product-flow":
            summary = _regenerate_product_flow(
                Path(args.flow_file).resolve(),
                Path(args.out_file).resolve(),
                overrides_file=Path(args.overrides_file).resolve() if args.overrides_file else None,
                allow_validation_fallback=bool(args.allow_validation_fallback),
            )
            _print_json(summary)
            return 0

    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"status": "error", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
