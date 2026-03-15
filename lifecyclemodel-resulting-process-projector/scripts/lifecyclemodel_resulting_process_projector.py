#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REQUEST_SCHEMA_PATH = SKILL_DIR / "assets" / "request.schema.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def first_non_empty(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def resolve_local_ref(root_schema: dict[str, Any], ref: str) -> dict[str, Any]:
    if not ref.startswith("#/"):
        raise ValueError(f"Only local schema refs are supported: {ref}")
    node: Any = root_schema
    for part in ref[2:].split("/"):
        node = node[part]
    if not isinstance(node, dict):
        raise ValueError(f"Schema ref does not resolve to an object: {ref}")
    return node


def _matches_type(value: Any, schema_type: str) -> bool:
    if schema_type == "object":
        return isinstance(value, dict)
    if schema_type == "array":
        return isinstance(value, list)
    if schema_type == "string":
        return isinstance(value, str)
    if schema_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if schema_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if schema_type == "boolean":
        return isinstance(value, bool)
    if schema_type == "null":
        return value is None
    return True


def _schema_is_valid(value: Any, schema: dict[str, Any], root_schema: dict[str, Any]) -> bool:
    return not validate_against_schema(value, schema, root_schema=root_schema)


def validate_against_schema(
    value: Any,
    schema: dict[str, Any],
    *,
    root_schema: dict[str, Any] | None = None,
    path: str = "$",
) -> list[str]:
    root = root_schema or schema
    errors: list[str] = []

    if "$ref" in schema:
        return validate_against_schema(
            value,
            resolve_local_ref(root, str(schema["$ref"])),
            root_schema=root,
            path=path,
        )

    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        if not any(_schema_is_valid(value, candidate, root) for candidate in any_of):
            errors.append(f"{path}: value does not satisfy anyOf")
        return errors

    all_of = schema.get("allOf")
    if isinstance(all_of, list):
        for candidate in all_of:
            errors.extend(validate_against_schema(value, candidate, root_schema=root, path=path))

    if_schema = schema.get("if")
    then_schema = schema.get("then")
    if isinstance(if_schema, dict) and isinstance(then_schema, dict):
        if _schema_is_valid(value, if_schema, root):
            errors.extend(validate_against_schema(value, then_schema, root_schema=root, path=path))

    schema_type = schema.get("type")
    if isinstance(schema_type, str) and not _matches_type(value, schema_type):
        return [f"{path}: expected {schema_type}, got {type(value).__name__}"]

    enum_values = schema.get("enum")
    if isinstance(enum_values, list) and value not in enum_values:
        errors.append(f"{path}: expected one of {enum_values}, got {value!r}")

    if "const" in schema and value != schema.get("const"):
        errors.append(f"{path}: expected const {schema.get('const')!r}, got {value!r}")

    if isinstance(value, str):
        min_length = schema.get("minLength")
        if isinstance(min_length, int) and len(value) < min_length:
            errors.append(f"{path}: string shorter than minLength={min_length}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        if isinstance(minimum, (int, float)) and value < minimum:
            errors.append(f"{path}: number smaller than minimum={minimum}")

    if isinstance(value, list):
        items_schema = schema.get("items")
        if isinstance(items_schema, dict):
            for index, item in enumerate(value):
                errors.extend(
                    validate_against_schema(
                        item,
                        items_schema,
                        root_schema=root,
                        path=f"{path}[{index}]",
                    )
                )

    if isinstance(value, dict):
        required = schema.get("required") or []
        for field in required:
            if field not in value:
                errors.append(f"{path}: missing required field '{field}'")

        properties = schema.get("properties")
        if isinstance(properties, dict):
            for key, subschema in properties.items():
                if key in value and isinstance(subschema, dict):
                    errors.extend(
                        validate_against_schema(
                            value[key],
                            subschema,
                            root_schema=root,
                            path=f"{path}.{key}",
                        )
                    )

            additional = schema.get("additionalProperties", True)
            if additional is False:
                unexpected = sorted(set(value) - set(properties))
                for key in unexpected:
                    errors.append(f"{path}: unexpected property '{key}'")
            elif isinstance(additional, dict):
                for key in sorted(set(value) - set(properties)):
                    errors.extend(
                        validate_against_schema(
                            value[key],
                            additional,
                            root_schema=root,
                            path=f"{path}.{key}",
                        )
                    )

    return errors


def validate_request_schema(request: dict[str, Any]) -> None:
    schema = load_json(REQUEST_SCHEMA_PATH)
    errors = validate_against_schema(request, schema)
    source_model = request.get("source_model") or {}
    if not any(
        source_model.get(field)
        for field in ("id", "json_ordered", "json_ordered_path")
    ):
        errors.append("$.source_model: provide at least one of id/json_ordered/json_ordered_path")
    if errors:
        raise ValueError("request schema validation failed:\n- " + "\n- ".join(errors))


def resolve_path(base_dir: Path, raw: str | None) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if "://" in text or text.startswith("file:"):
        return text
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    else:
        path = path.resolve()
    return str(path)


def resolve_name_field(name_payload: Any) -> str | None:
    if isinstance(name_payload, str):
        return name_payload.strip() or None
    if not isinstance(name_payload, dict):
        return None
    for key in ("baseName", "shortName", "name"):
        value = name_payload.get(key)
        if isinstance(value, dict):
            text = first_non_empty(
                value.get("@index"),
                value.get("#text"),
                value.get("text"),
            )
            if text:
                return text
        text = first_non_empty(value)
        if text:
            return text
    return None


def model_identifier(model: dict[str, Any], source_model: dict[str, Any]) -> tuple[str, str, str]:
    info = model.get("lifeCycleModelInformation") or {}
    data_info = info.get("dataSetInformation") or {}
    model_id = first_non_empty(
        source_model.get("id"),
        model.get("@id"),
        model.get("id"),
        data_info.get("identifierOfSubDataSet"),
    ) or f"lm-{sha256_text(canonical_json(model))[:12]}"
    version = first_non_empty(
        source_model.get("version"),
        model.get("@version"),
        model.get("version"),
        data_info.get("@version"),
    ) or "00.00.001"
    name = first_non_empty(
        source_model.get("name"),
        resolve_name_field(data_info.get("name")),
        model.get("name"),
        model_id,
    ) or model_id
    return model_id, version, name


def extract_process_instances(model: dict[str, Any]) -> list[dict[str, Any]]:
    technology = model.get("technology") or {}
    processes = technology.get("processes") or {}
    instances = ensure_list(processes.get("processInstance"))
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(instances, start=1):
        if not isinstance(item, dict):
            continue
        ref = item.get("referenceToProcess") or {}
        normalized.append(
            {
                "instance_id": first_non_empty(item.get("@id"), item.get("id"), f"pi-{index}") or f"pi-{index}",
                "process_id": first_non_empty(ref.get("@refObjectId"), ref.get("id"), ref.get("processId")) or f"proc-{index}",
                "process_version": first_non_empty(ref.get("@version"), ref.get("version")) or "00.00.001",
                "label": first_non_empty(ref.get("shortDescription"), ref.get("name"), ref.get("@refObjectId")) or f"process-{index}",
                "raw": item,
            }
        )
    return normalized


def extract_edges(model: dict[str, Any]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for instance in extract_process_instances(model):
        raw = instance["raw"]
        connections = raw.get("connections") or {}
        outputs = ensure_list(connections.get("outputExchange"))
        for edge_index, exchange in enumerate(outputs, start=1):
            if not isinstance(exchange, dict):
                continue
            downstream = exchange.get("downstreamProcess") or {}
            downstream_id = first_non_empty(
                downstream.get("@refObjectId"),
                downstream.get("id"),
                exchange.get("downstreamProcessId"),
            )
            if not downstream_id:
                continue
            edges.append(
                {
                    "edge_id": first_non_empty(exchange.get("@id"), exchange.get("id"), f"{instance['instance_id']}-edge-{edge_index}") or f"{instance['instance_id']}-edge-{edge_index}",
                    "from": instance["instance_id"],
                    "to": downstream_id,
                    "exchange_id": first_non_empty(exchange.get("@id"), exchange.get("id")),
                }
            )
    return edges


def normalize_request(
    request: dict[str, Any],
    *,
    base_dir: Path,
) -> dict[str, Any]:
    normalized = copy.deepcopy(request)
    source_model = normalized.setdefault("source_model", {})
    source_model["json_ordered_path"] = resolve_path(base_dir, source_model.get("json_ordered_path"))
    projection = normalized.setdefault("projection", {})
    projection.setdefault("mode", "primary-only")
    projection["attach_graph_snapshot_uri"] = resolve_path(
        base_dir,
        projection.get("attach_graph_snapshot_uri"),
    )
    publish = normalized.setdefault("publish", {})
    publish.setdefault("intent", "dry_run")
    publish.setdefault("prepare_process_payloads", True)
    publish.setdefault("prepare_relation_payloads", True)
    return normalized


def synthesize_request_from_model(
    *,
    model_file: Path,
    projection_mode: str,
) -> dict[str, Any]:
    return {
        "source_model": {
            "json_ordered_path": str(model_file.resolve()),
        },
        "projection": {
            "mode": projection_mode,
            "metadata_overrides": {},
            "attach_graph_snapshot": False,
        },
        "publish": {
            "intent": "dry_run",
            "prepare_process_payloads": True,
            "prepare_relation_payloads": True,
        },
    }


def load_request_from_args(args: argparse.Namespace) -> tuple[dict[str, Any], Path]:
    if args.request:
        request_path = Path(args.request).expanduser().resolve()
        request = load_json(request_path)
        base_dir = request_path.parent
    else:
        if not args.model_file:
            raise ValueError("Either --request or --model-file is required.")
        request = synthesize_request_from_model(
            model_file=Path(args.model_file).expanduser().resolve(),
            projection_mode="all-subproducts" if args.projection_role == "all" else "primary-only",
        )
        base_dir = Path.cwd()
    validate_request_schema(request)
    return normalize_request(request, base_dir=base_dir), base_dir


def load_source_model(normalized_request: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    source_model = normalized_request.get("source_model") or {}
    embedded = source_model.get("json_ordered")
    if isinstance(embedded, dict):
        return copy.deepcopy(embedded), None
    model_path = source_model.get("json_ordered_path")
    if model_path:
        resolved = Path(str(model_path)).expanduser().resolve()
        return load_json(resolved), str(resolved)
    raise ValueError("source_model must include json_ordered or json_ordered_path for local projection.")


def reference_to_resulting_process(model: dict[str, Any]) -> tuple[str | None, str | None]:
    info = model.get("lifeCycleModelInformation") or {}
    data_info = info.get("dataSetInformation") or {}
    ref = data_info.get("referenceToResultingProcess") or {}
    if not isinstance(ref, dict):
        return None, None
    return (
        first_non_empty(ref.get("@refObjectId"), ref.get("id")),
        first_non_empty(ref.get("@version"), ref.get("version")),
    )


def reference_process_instance_id(model: dict[str, Any]) -> str | None:
    info = model.get("lifeCycleModelInformation") or {}
    quantitative = info.get("quantitativeReference") or {}
    ref = quantitative.get("referenceToReferenceProcess") or {}
    if isinstance(ref, dict):
        return first_non_empty(ref.get("@refObjectId"), ref.get("id"))
    return first_non_empty(ref)


def build_process_payload(
    *,
    source_model_id: str,
    source_model_version: str,
    source_model_name: str,
    process_id: str,
    process_version: str,
    role: str,
    projection_signature: str,
    process_instances: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    metadata_overrides: dict[str, Any],
    attach_graph_snapshot_uri: str | None,
) -> dict[str, Any]:
    payload_name = source_model_name if role == "primary" else f"{source_model_name} - {role}"
    metadata = {
        "generated_from_lifecyclemodel_id": source_model_id,
        "generated_from_lifecyclemodel_version": source_model_version,
        "projection_role": role,
        "projection_signature": projection_signature,
    }
    metadata.update(metadata_overrides)
    if attach_graph_snapshot_uri:
        metadata["graph_snapshot_uri"] = attach_graph_snapshot_uri
    return {
        "@type": "process",
        "@id": process_id,
        "@version": process_version,
        "processInformation": {
            "dataSetInformation": {
                "name": {
                    "baseName": payload_name,
                },
                "generatedFromLifecycleModel": {
                    "id": source_model_id,
                    "version": source_model_version,
                    "role": role,
                },
            },
        },
        "projectionMetadata": metadata,
        "topologySummary": {
            "process_instance_count": len(process_instances),
            "edge_count": len(edges),
        },
    }


def build_projection_bundle(
    normalized_request: dict[str, Any],
    source_model_json: dict[str, Any],
    *,
    model_path: str | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_model_cfg = normalized_request.get("source_model") or {}
    projection = normalized_request.get("projection") or {}
    publish = normalized_request.get("publish") or {}
    source_model_id, source_model_version, source_model_name = model_identifier(
        source_model_json,
        source_model_cfg,
    )
    process_instances = extract_process_instances(source_model_json)
    edges = extract_edges(source_model_json)
    ref_process_id, ref_process_version = reference_to_resulting_process(source_model_json)
    ref_process_instance = reference_process_instance_id(source_model_json)
    signature_seed = {
        "source_model_id": source_model_id,
        "source_model_version": source_model_version,
        "projection_mode": projection.get("mode"),
        "process_instances": [
            {
                "instance_id": item["instance_id"],
                "process_id": item["process_id"],
                "process_version": item["process_version"],
            }
            for item in process_instances
        ],
        "edges": edges,
    }
    base_signature = f"sha256:{sha256_text(canonical_json(signature_seed))}"
    metadata_overrides = dict(projection.get("metadata_overrides") or {})
    attach_graph_snapshot_uri = projection.get("attach_graph_snapshot_uri")
    primary_process_id = first_non_empty(
        projection.get("process_id"),
        ref_process_id,
        f"{source_model_id}-resulting-process",
    ) or f"{source_model_id}-resulting-process"
    primary_process_version = first_non_empty(
        projection.get("process_version"),
        ref_process_version,
        source_model_version,
    ) or source_model_version
    projected_processes: list[dict[str, Any]] = []
    relations: list[dict[str, Any]] = []

    primary_payload = build_process_payload(
        source_model_id=source_model_id,
        source_model_version=source_model_version,
        source_model_name=source_model_name,
        process_id=primary_process_id,
        process_version=primary_process_version,
        role="primary",
        projection_signature=base_signature,
        process_instances=process_instances,
        edges=edges,
        metadata_overrides=metadata_overrides,
        attach_graph_snapshot_uri=attach_graph_snapshot_uri,
    )
    projected_processes.append(
        {
            "role": "primary",
            "id": primary_process_id,
            "version": primary_process_version,
            "name": source_model_name,
            "json_ordered": primary_payload,
            "metadata": primary_payload["projectionMetadata"],
        }
    )
    relations.append(
        {
            "lifecyclemodel_id": source_model_id,
            "lifecyclemodel_version": source_model_version,
            "resulting_process_id": primary_process_id,
            "resulting_process_version": primary_process_version,
            "projection_role": "primary",
            "projection_signature": base_signature,
            "is_primary": True,
        }
    )

    if projection.get("mode") == "all-subproducts":
        json_tg = source_model_json.get("json_tg") or {}
        submodels = ensure_list(json_tg.get("submodels"))
        for index, submodel in enumerate(submodels, start=1):
            if not isinstance(submodel, dict):
                continue
            role = f"secondary-{index}"
            secondary_process_id = (
                first_non_empty(
                    submodel.get("resultingProcessId"),
                    submodel.get("resulting_process_id"),
                )
                or f"{source_model_id}-secondary-{index}"
            )
            secondary_signature = f"sha256:{sha256_text(base_signature + role + secondary_process_id)}"
            secondary_payload = build_process_payload(
                source_model_id=source_model_id,
                source_model_version=source_model_version,
                source_model_name=source_model_name,
                process_id=secondary_process_id,
                process_version=primary_process_version,
                role=role,
                projection_signature=secondary_signature,
                process_instances=process_instances,
                edges=edges,
                metadata_overrides=metadata_overrides,
                attach_graph_snapshot_uri=attach_graph_snapshot_uri,
            )
            projected_processes.append(
                {
                    "role": role,
                    "id": secondary_process_id,
                    "version": primary_process_version,
                    "name": first_non_empty(submodel.get("label"), submodel.get("modelName"), role) or role,
                    "json_ordered": secondary_payload,
                    "metadata": secondary_payload["projectionMetadata"],
                }
            )
            relations.append(
                {
                    "lifecyclemodel_id": source_model_id,
                    "lifecyclemodel_version": source_model_version,
                    "resulting_process_id": secondary_process_id,
                    "resulting_process_version": primary_process_version,
                    "projection_role": role,
                    "projection_signature": secondary_signature,
                    "is_primary": False,
                }
            )

    source_model_summary = {
        "id": source_model_id,
        "version": source_model_version,
        "name": source_model_name,
        "json_ordered_path": model_path,
        "reference_to_resulting_process_id": ref_process_id,
        "reference_to_resulting_process_version": ref_process_version,
        "reference_process_instance_id": ref_process_instance,
    }
    report = {
        "generated_at": now_iso(),
        "status": (
            "projected_local_bundle"
            if publish.get("intent") == "publish"
            else "prepared_local_bundle"
        ),
        "source_model": source_model_summary,
        "projection_mode": projection.get("mode", "primary-only"),
        "node_count": len(process_instances),
        "edge_count": len(edges),
        "reference_process_instance_id": ref_process_instance,
        "process_instance_preview": [
            {
                "instance_id": item["instance_id"],
                "process_id": item["process_id"],
                "label": item["label"],
            }
            for item in process_instances[:10]
        ],
        "edge_preview": edges[:10],
        "projection_signature": base_signature,
        "attach_graph_snapshot_uri": attach_graph_snapshot_uri,
        "notes": [
            "This local projector materializes topology-aware projection bundles and relation payloads.",
            "Remote writes remain gated behind an explicit publish layer.",
        ],
    }
    bundle = {
        "source_model": source_model_summary,
        "projected_processes": projected_processes,
        "relations": relations,
        "report": report,
        "projection": {
            "mode": projection.get("mode", "primary-only"),
            "metadata_overrides": metadata_overrides,
            "attach_graph_snapshot_uri": attach_graph_snapshot_uri,
        },
    }
    return bundle, report, source_model_summary


def write_projection_artifacts(
    *,
    out_dir: Path,
    normalized_request: dict[str, Any],
    source_model_json: dict[str, Any],
    source_model_summary: dict[str, Any],
    bundle: dict[str, Any],
    report: dict[str, Any],
) -> None:
    dump_json(out_dir / "request.normalized.json", normalized_request)
    dump_json(out_dir / "source-model.normalized.json", source_model_json)
    dump_json(out_dir / "source-model.summary.json", source_model_summary)
    dump_json(out_dir / "projection-report.json", report)
    dump_json(out_dir / "process-projection-bundle.json", bundle)


def cmd_prepare_or_project(args: argparse.Namespace) -> int:
    normalized_request, _ = load_request_from_args(args)
    out_dir = Path(args.out_dir).expanduser().resolve()
    source_model_json, model_path = load_source_model(normalized_request)
    bundle, report, source_model_summary = build_projection_bundle(
        normalized_request,
        source_model_json,
        model_path=model_path,
    )
    write_projection_artifacts(
        out_dir=out_dir,
        normalized_request=normalized_request,
        source_model_json=source_model_json,
        source_model_summary=source_model_summary,
        bundle=bundle,
        report=report,
    )
    print(
        json.dumps(
            {
                "ok": True,
                "command": args.command,
                "out_dir": str(out_dir),
                "projected_process_count": len(bundle["projected_processes"]),
                "relation_count": len(bundle["relations"]),
                "status": report["status"],
            },
            ensure_ascii=False,
        )
    )
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    bundle_path = run_dir / "process-projection-bundle.json"
    report_path = run_dir / "projection-report.json"
    if not bundle_path.exists():
        raise ValueError(f"Missing projection bundle: {bundle_path}")
    if not report_path.exists():
        raise ValueError(f"Missing projection report: {report_path}")
    bundle = load_json(bundle_path)
    report = load_json(report_path)
    publish_bundle = {
        "generated_at": now_iso(),
        "run_dir": str(run_dir),
        "source_model": bundle.get("source_model") or {},
        "publish_processes": bool(args.publish_processes),
        "publish_relations": bool(args.publish_relations),
        "status": "prepared_local_publish_bundle",
        "projected_processes": bundle.get("projected_processes") if args.publish_processes else [],
        "relations": bundle.get("relations") if args.publish_relations else [],
        "report": report,
    }
    dump_json(run_dir / "publish-bundle.json", publish_bundle)
    dump_json(
        run_dir / "publish-intent.json",
        {
            "ok": True,
            "command": "publish",
            "run_dir": str(run_dir),
            "publish_processes": bool(args.publish_processes),
            "publish_relations": bool(args.publish_relations),
            "status": "prepared_local_publish_bundle",
        },
    )
    print(json.dumps(publish_bundle, ensure_ascii=False))
    return 0


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Lifecycle model resulting process projector")
    sub = p.add_subparsers(dest="command", required=True)

    prepare = sub.add_parser("prepare")
    prepare.add_argument("--request")
    prepare.add_argument("--model-file")
    prepare.add_argument("--out-dir", required=True)
    prepare.add_argument("--projection-role", choices=["primary", "all"], default="primary")
    prepare.set_defaults(func=cmd_prepare_or_project)

    project = sub.add_parser("project")
    project.add_argument("--request")
    project.add_argument("--model-file")
    project.add_argument("--out-dir", required=True)
    project.add_argument("--projection-role", choices=["primary", "all"], default="primary")
    project.set_defaults(func=cmd_prepare_or_project)

    publish = sub.add_parser("publish")
    publish.add_argument("--run-dir", required=True)
    publish.add_argument("--publish-processes", action="store_true")
    publish.add_argument("--publish-relations", action="store_true")
    publish.set_defaults(func=cmd_publish)
    return p


if __name__ == "__main__":
    parsed = parser().parse_args()
    raise SystemExit(parsed.func(parsed))
