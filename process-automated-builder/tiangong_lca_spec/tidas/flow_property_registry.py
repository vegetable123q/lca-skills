"""Registry utilities for flow property and unit-group lookups."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
MAPPING_PATH = SCHEMA_DIR / "flowproperty_unitgroup_mapping.json"

_SPEC_ROOT = Path(__file__).resolve().parents[1]
_RESOURCE_DIR = _SPEC_ROOT / "resources"
_RESOURCE_FLOWPROPERTY_DIR = _RESOURCE_DIR / "flowproperties"
_RESOURCE_UNIT_DIR = _RESOURCE_DIR / "units"

FLOW_PROPERTY_VERSION_OVERRIDES: Mapping[str, str] = {
    # Mass property ships with a published ILCD dataset version.
    "93a60a56-a3c8-11da-a746-0800200b9a66": "03.00.003",
    # mass*distance (kg*km) transport service property.
    "118f2a40-50ec-457c-aa60-9bc6b6af9931": "01.01.000",
}
DEFAULT_FLOW_PROPERTY_VERSION = "01.01.000"

_VERSION_RE = re.compile(r"^(?P<uuid>.+?)_(?P<version>\d+\.\d+\.\d+)\.json$")


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


def _extract_name_text(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, Mapping):
                continue
            if _coerce_text(item.get("@xml:lang")).lower() == "en":
                text = _coerce_text(item.get("#text"))
                if text:
                    return text
        for item in value:
            if isinstance(item, Mapping):
                text = _coerce_text(item.get("#text"))
                if text:
                    return text
        return ""
    return _coerce_text(value)


def _extract_classification(value: Any) -> tuple[str, ...]:
    if isinstance(value, Mapping):
        nested = value.get("common:classification") or value.get("classification")
        if nested is not None:
            return _extract_classification(nested)
        classes = value.get("common:class") or value.get("class")
        if classes is not None:
            return _extract_classification(classes)
    if isinstance(value, list):
        labels: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                label = _coerce_text(item.get("#text") or item.get("text"))
            else:
                label = _coerce_text(item)
            if label:
                labels.append(label)
        return tuple(labels)
    if isinstance(value, Mapping):
        label = _coerce_text(value.get("#text") or value.get("text"))
        return (label,) if label else ()
    text = _coerce_text(value)
    return (text,) if text else ()


def _normalize_name(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_unit_token(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).strip().lower()
    text = text.replace(" ", "")
    text = text.replace("^", "")
    text = text.replace("³", "3")
    text = text.replace("·", "*")
    text = text.replace("×", "*")
    return text


def _version_key(version: str | None) -> tuple[int, int, int]:
    if not version:
        return (0, 0, 0)
    parts = str(version).split(".")
    numbers: list[int] = []
    for part in parts[:3]:
        try:
            numbers.append(int(part))
        except ValueError:
            numbers.append(0)
    while len(numbers) < 3:
        numbers.append(0)
    return (numbers[0], numbers[1], numbers[2])


def _filename_uuid_version(path: Path) -> tuple[str, str | None]:
    match = _VERSION_RE.match(path.name)
    if not match:
        return path.stem, None
    return match.group("uuid"), match.group("version")


def _parse_amount_text(value: Any) -> str:
    text = _coerce_text(value)
    if not text:
        return ""
    return text.replace(",", "")


@dataclass(slots=True, frozen=True)
class UnitDescriptor:
    """Single unit entry inside a unit group."""

    name: str
    mean_value: str
    internal_id: str
    general_comment: str | None
    is_reference: bool


@dataclass(slots=True, frozen=True)
class UnitGroupDescriptor:
    """Description of a unit group associated with a flow property."""

    uuid: str
    name: str
    reference_internal_id: str
    units: tuple[UnitDescriptor, ...]
    version: str = DEFAULT_FLOW_PROPERTY_VERSION

    @property
    def reference_unit(self) -> UnitDescriptor | None:
        for unit in self.units:
            if unit.is_reference:
                return unit
        return self.units[0] if self.units else None


@dataclass(slots=True, frozen=True)
class FlowPropertyDescriptor:
    """Flow property metadata with linked unit group."""

    uuid: str
    name: str
    classification: tuple[str, ...]
    reference_unit_description: str
    reference_unit_group_uuid: str
    unit_group: UnitGroupDescriptor
    version: str = DEFAULT_FLOW_PROPERTY_VERSION


class FlowPropertyRegistry:
    """Loads and indexes flow property descriptors for quick lookups."""

    def __init__(
        self,
        mapping_path: Path | None = None,
        *,
        flowproperties_dir: Path | None = None,
        units_dir: Path | None = None,
    ) -> None:
        self._mapping_path = mapping_path or MAPPING_PATH
        self._flowproperties_dir = flowproperties_dir or _RESOURCE_FLOWPROPERTY_DIR
        self._units_dir = units_dir or _RESOURCE_UNIT_DIR
        self._properties = self._load()
        self._by_uuid = {descriptor.uuid.lower(): descriptor for descriptor in self._properties}
        self._by_name = {_normalize_name(descriptor.name): descriptor for descriptor in self._properties if descriptor.name}
        self._by_unit_token: dict[str, tuple[FlowPropertyDescriptor, ...]] = self._build_unit_index(self._properties)

    def _load(self) -> tuple[FlowPropertyDescriptor, ...]:
        resource_descriptors = self._load_from_resources()
        if resource_descriptors:
            # Merge any mapping-only entries (if present) without overriding resource-derived descriptors.
            merged = {descriptor.uuid.lower(): descriptor for descriptor in resource_descriptors}
            for descriptor in self._load_from_mapping():
                merged.setdefault(descriptor.uuid.lower(), descriptor)
            return tuple(sorted(merged.values(), key=lambda item: (_normalize_name(item.name), item.uuid.lower())))

        mapped = self._load_from_mapping()
        if not mapped:
            raise FileNotFoundError(
                "No flow property registry source found (resources and mapping both unavailable/empty)."
            )
        return tuple(sorted(mapped, key=lambda item: (_normalize_name(item.name), item.uuid.lower())))

    def _load_from_mapping(self) -> tuple[FlowPropertyDescriptor, ...]:
        if not self._mapping_path.exists():
            return ()
        raw = json.loads(self._mapping_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            return ()
        descriptors: list[FlowPropertyDescriptor] = []
        for entry in raw:
            if not isinstance(entry, Mapping):
                continue
            unit_group_raw = entry.get("unit_group") or {}
            units: list[UnitDescriptor] = []
            if isinstance(unit_group_raw, Mapping):
                for unit in unit_group_raw.get("units") or ():
                    if not isinstance(unit, Mapping):
                        continue
                    units.append(
                        UnitDescriptor(
                            name=_coerce_text(unit.get("name")),
                            mean_value=_parse_amount_text(unit.get("mean_value")),
                            internal_id=_coerce_text(unit.get("internal_id")),
                            general_comment=_coerce_text(unit.get("general_comment")) or None,
                            is_reference=bool(unit.get("is_reference")),
                        )
                    )
            unit_group = UnitGroupDescriptor(
                uuid=_coerce_text(unit_group_raw.get("uuid")) if isinstance(unit_group_raw, Mapping) else "",
                name=_coerce_text(unit_group_raw.get("name")) if isinstance(unit_group_raw, Mapping) else "",
                reference_internal_id=(
                    _coerce_text(unit_group_raw.get("reference_internal_id")) if isinstance(unit_group_raw, Mapping) else ""
                )
                or "0",
                units=tuple(units),
            )
            descriptors.append(
                FlowPropertyDescriptor(
                    uuid=_coerce_text(entry.get("flow_property_uuid")),
                    name=_coerce_text(entry.get("flow_property_name")),
                    classification=tuple(entry.get("flow_property_classification") or ()),
                    reference_unit_description=_coerce_text(entry.get("flow_property_reference_unit_description")),
                    reference_unit_group_uuid=_coerce_text(entry.get("reference_unit_group_uuid")),
                    unit_group=unit_group,
                    version=FLOW_PROPERTY_VERSION_OVERRIDES.get(_coerce_text(entry.get("flow_property_uuid")), DEFAULT_FLOW_PROPERTY_VERSION),
                )
            )
        return tuple(descriptor for descriptor in descriptors if descriptor.uuid)

    def _load_from_resources(self) -> tuple[FlowPropertyDescriptor, ...]:
        if not self._flowproperties_dir.exists() or not self._units_dir.exists():
            return ()

        unit_groups, unit_group_aliases = self._load_unit_groups_from_resources()
        by_uuid: dict[str, FlowPropertyDescriptor] = {}
        for path in sorted(self._flowproperties_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            dataset = payload.get("flowPropertyDataSet")
            if not isinstance(dataset, Mapping):
                continue
            info = dataset.get("flowPropertiesInformation")
            if not isinstance(info, Mapping):
                continue
            data_info = info.get("dataSetInformation")
            if not isinstance(data_info, Mapping):
                data_info = {}
            file_uuid, file_version = _filename_uuid_version(path)
            uuid_value = _coerce_text(data_info.get("common:UUID")) or file_uuid
            if not uuid_value:
                continue
            name = _extract_name_text(data_info.get("common:name")) or uuid_value
            quant_ref = info.get("quantitativeReference")
            if not isinstance(quant_ref, Mapping):
                quant_ref = {}
            ref_group = quant_ref.get("referenceToReferenceUnitGroup")
            if not isinstance(ref_group, Mapping):
                ref_group = {}
            unit_group_uuid = _coerce_text(ref_group.get("@refObjectId"))
            unit_group = unit_group_aliases.get(unit_group_uuid.lower()) if unit_group_uuid else None
            if unit_group is None:
                unit_group = UnitGroupDescriptor(
                    uuid=unit_group_uuid or "",
                    name=_extract_name_text(ref_group.get("common:shortDescription")) or unit_group_uuid or "",
                    reference_internal_id="0",
                    units=(),
                )
            classification = _extract_classification(data_info.get("classificationInformation"))
            ref_unit_desc = ""
            if unit_group.reference_unit is not None:
                ref_unit_desc = unit_group.reference_unit.name
            if not ref_unit_desc:
                ref_unit_desc = _extract_name_text(ref_group.get("common:shortDescription")) or unit_group.name
            descriptor = FlowPropertyDescriptor(
                uuid=uuid_value,
                name=name,
                classification=classification,
                reference_unit_description=ref_unit_desc,
                reference_unit_group_uuid=unit_group.uuid or unit_group_uuid,
                unit_group=unit_group,
                version=file_version or FLOW_PROPERTY_VERSION_OVERRIDES.get(uuid_value, DEFAULT_FLOW_PROPERTY_VERSION),
            )
            key = uuid_value.lower()
            previous = by_uuid.get(key)
            if previous is None or _version_key(descriptor.version) >= _version_key(previous.version):
                by_uuid[key] = descriptor
        return tuple(by_uuid.values())

    def _load_unit_groups_from_resources(self) -> tuple[dict[str, UnitGroupDescriptor], dict[str, UnitGroupDescriptor]]:
        by_actual_uuid: dict[str, UnitGroupDescriptor] = {}
        aliases: dict[str, UnitGroupDescriptor] = {}
        for path in sorted(self._units_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            dataset = payload.get("unitGroupDataSet")
            if not isinstance(dataset, Mapping):
                continue
            info = dataset.get("unitGroupInformation")
            if not isinstance(info, Mapping):
                continue
            data_info = info.get("dataSetInformation")
            if not isinstance(data_info, Mapping):
                data_info = {}
            file_uuid, file_version = _filename_uuid_version(path)
            actual_uuid = _coerce_text(data_info.get("common:UUID")) or file_uuid
            if not actual_uuid:
                continue
            name = _extract_name_text(data_info.get("common:name")) or actual_uuid
            quant_ref = info.get("quantitativeReference")
            if not isinstance(quant_ref, Mapping):
                quant_ref = {}
            ref_internal_id = _coerce_text(quant_ref.get("referenceToReferenceUnit")) or "0"
            units_block = dataset.get("units")
            if not isinstance(units_block, Mapping):
                units_block = {}
            raw_units = units_block.get("unit")
            units_list = raw_units if isinstance(raw_units, list) else [raw_units] if isinstance(raw_units, Mapping) else []
            units: list[UnitDescriptor] = []
            for raw_unit in units_list:
                if not isinstance(raw_unit, Mapping):
                    continue
                internal_id = _coerce_text(raw_unit.get("@dataSetInternalID"))
                units.append(
                    UnitDescriptor(
                        name=_coerce_text(raw_unit.get("name")),
                        mean_value=_parse_amount_text(raw_unit.get("meanValue")),
                        internal_id=internal_id,
                        general_comment=_extract_name_text(raw_unit.get("generalComment")) or None,
                        is_reference=bool(ref_internal_id and internal_id and internal_id == ref_internal_id),
                    )
                )
            descriptor = UnitGroupDescriptor(
                uuid=actual_uuid,
                name=name,
                reference_internal_id=ref_internal_id,
                units=tuple(unit for unit in units if unit.name),
                version=file_version or DEFAULT_FLOW_PROPERTY_VERSION,
            )
            key = actual_uuid.lower()
            previous = by_actual_uuid.get(key)
            if previous is None or _version_key(descriptor.version) >= _version_key(previous.version):
                by_actual_uuid[key] = descriptor
            aliases[key] = descriptor
            if file_uuid:
                aliases[file_uuid.lower()] = descriptor
        return by_actual_uuid, aliases

    @staticmethod
    def _build_unit_index(properties: tuple[FlowPropertyDescriptor, ...]) -> dict[str, tuple[FlowPropertyDescriptor, ...]]:
        index: dict[str, list[FlowPropertyDescriptor]] = {}
        for descriptor in properties:
            seen_tokens: set[str] = set()
            for unit in descriptor.unit_group.units:
                token = _normalize_unit_token(unit.name)
                if not token or token in seen_tokens:
                    continue
                seen_tokens.add(token)
                index.setdefault(token, []).append(descriptor)
        return {
            token: tuple(sorted(values, key=lambda item: (_normalize_name(item.name), item.uuid.lower())))
            for token, values in index.items()
        }

    def list(self) -> tuple[FlowPropertyDescriptor, ...]:
        """Return every flow property descriptor."""
        return self._properties

    def get(self, uuid: str) -> FlowPropertyDescriptor:
        """Return descriptor by UUID (case insensitive)."""
        descriptor = self._by_uuid.get(_coerce_text(uuid).lower())
        if descriptor is None:
            raise KeyError(f"Unknown flow property UUID: {uuid}")
        return descriptor

    def get_version(self, uuid: str) -> str:
        """Return the best-known dataset version for a flow property UUID."""
        descriptor = self.get(uuid)
        return FLOW_PROPERTY_VERSION_OVERRIDES.get(descriptor.uuid, descriptor.version or DEFAULT_FLOW_PROPERTY_VERSION)

    def find(self, name: str) -> FlowPropertyDescriptor | None:
        """Return descriptor by exact name (case insensitive, normalized spaces)."""
        return self._by_name.get(_normalize_name(name))

    def search_by_unit(self, unit_name: str) -> list[FlowPropertyDescriptor]:
        """Find potential flow properties that contain the given unit (normalized)."""
        token = _normalize_unit_token(unit_name)
        return list(self._by_unit_token.get(token, ()))

    def compatible_property_uuids_for_unit(self, unit_name: str) -> tuple[str, ...]:
        """Return candidate property UUIDs compatible with the given unit token."""
        return tuple(descriptor.uuid for descriptor in self.search_by_unit(unit_name))

    def fuzzy_match(self, text: str) -> FlowPropertyDescriptor | None:
        """Attempt to match property name against free text."""
        candidate_raw = _coerce_text(text)
        candidate = _normalize_name(candidate_raw)
        if not candidate:
            return None
        direct = self._by_name.get(candidate)
        if direct:
            return direct

        best: FlowPropertyDescriptor | None = None
        best_score = -1
        for descriptor in self._properties:
            name = _normalize_name(descriptor.name)
            score = 0
            if not name:
                continue
            if name in candidate:
                score += 8 + len(name)
            if candidate in name:
                score += 4 + len(candidate)
            if descriptor.reference_unit_description:
                ref_unit = _normalize_unit_token(descriptor.reference_unit_description)
                if ref_unit and ref_unit in _normalize_unit_token(candidate_raw):
                    score += 2
            if score > best_score:
                best_score = score
                best = descriptor
        return best if best_score > 0 else None

    def build_flow_property_block(
        self,
        flow_property_uuid: str,
        *,
        mean_value: str = "1.0",
        data_set_internal_id: str | None = None,
        version_override: str | None = None,
    ) -> dict[str, object]:
        """Return an ILCD-compatible flowProperties block."""
        descriptor = self.get(flow_property_uuid)
        version = version_override or self.get_version(descriptor.uuid)
        internal_id = data_set_internal_id or descriptor.unit_group.reference_internal_id or "0"
        reference = {
            "@type": "flow property data set",
            "@refObjectId": descriptor.uuid,
            "@uri": f"../flowproperties/{descriptor.uuid}.xml",
            "@version": version,
            "common:shortDescription": {
                "@xml:lang": "en",
                "#text": descriptor.name,
            },
        }
        return {
            "flowProperty": {
                "@dataSetInternalID": internal_id,
                "meanValue": mean_value,
                "referenceToFlowPropertyDataSet": reference,
            }
        }


@lru_cache(maxsize=1)
def get_default_registry() -> FlowPropertyRegistry:
    """Return a cached registry instance."""
    return FlowPropertyRegistry()
