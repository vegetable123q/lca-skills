"""Helpers for inferring elementary flow kind/compartment from TIDAS categories."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Mapping, Sequence

from .schema_loader import TidasSchemaRepository

_SCHEMA_FILENAME = "tidas_flows_elementary_category.json"

_EMISSION_ROOT = "1"
_RESOURCE_ROOT = "2"
_OTHER_ROOT = "4"


@dataclass(slots=True, frozen=True)
class ElementaryFlowClassificationEntry:
    """Single elementary category entry loaded from schema."""

    level: str
    cat_id: str
    text: str


class ElementaryFlowClassificationRegistry:
    """Load elementary category schema and infer kind/compartment labels."""

    def __init__(self, *, repository: TidasSchemaRepository | None = None) -> None:
        self._repository = repository or TidasSchemaRepository()
        self._entries_by_id = _load_entries(self._repository)

    def infer_kind_and_compartment(
        self,
        classification: Sequence[Mapping[str, Any] | None] | Mapping[str, Any] | None,
        *,
        category_path: str | None = None,
        general_comment: str | None = None,
    ) -> tuple[str | None, str | None]:
        cat_ids, texts = _collect_classification_fragments(classification, category_path=category_path, general_comment=general_comment)
        kind = self._infer_kind(cat_ids, texts)
        compartment = self._infer_compartment(cat_ids, texts)
        if kind != "emission":
            compartment = None
        return kind, compartment

    def _infer_kind(self, cat_ids: list[str], texts: list[str]) -> str | None:
        for cat_id in cat_ids:
            root = cat_id.split(".", 1)[0]
            if root == _EMISSION_ROOT:
                return "emission"
            if root == _RESOURCE_ROOT:
                return "resource"
            if root == _OTHER_ROOT:
                return "other"
            entry = self._entries_by_id.get(cat_id)
            if entry:
                root = entry.cat_id.split(".", 1)[0]
                if root == _EMISSION_ROOT:
                    return "emission"
                if root == _RESOURCE_ROOT:
                    return "resource"
                if root == _OTHER_ROOT:
                    return "other"

        combined = " ".join(texts).lower()
        if "resource" in combined:
            return "resource"
        if "emission" in combined:
            return "emission"
        if "other elementary" in combined:
            return "other"
        return None

    def _infer_compartment(self, cat_ids: list[str], texts: list[str]) -> str | None:
        for cat_id in cat_ids:
            if cat_id.startswith("1.1"):
                return "water"
            if cat_id.startswith("1.2"):
                return "soil"
            if cat_id.startswith("1.3"):
                return "air"
            entry = self._entries_by_id.get(cat_id)
            if not entry:
                continue
            text = entry.text.lower()
            if "to water" in text:
                return "water"
            if "to soil" in text:
                return "soil"
            if "to air" in text:
                return "air"

        combined = " ".join(texts).lower()
        if any(token in combined for token in (" to water", "emission to water", "emissions to water")):
            return "water"
        if any(token in combined for token in (" to soil", "emission to soil", "emissions to soil")):
            return "soil"
        if any(token in combined for token in (" to air", "emission to air", "emissions to air")):
            return "air"
        return None


def infer_elementary_kind_and_compartment(
    classification: Sequence[Mapping[str, Any] | None] | Mapping[str, Any] | None,
    *,
    category_path: str | None = None,
    general_comment: str | None = None,
) -> tuple[str | None, str | None]:
    """Infer elementary kind (`resource`/`emission`) and compartment."""
    registry = ElementaryFlowClassificationRegistry()
    return registry.infer_kind_and_compartment(
        classification,
        category_path=category_path,
        general_comment=general_comment,
    )


def _collect_classification_fragments(
    classification: Sequence[Mapping[str, Any] | None] | Mapping[str, Any] | None,
    *,
    category_path: str | None,
    general_comment: str | None,
) -> tuple[list[str], list[str]]:
    cat_ids: list[str] = []
    texts: list[str] = []

    def _push_cat_id(value: Any) -> None:
        text = str(value or "").strip()
        if text and text not in cat_ids:
            cat_ids.append(text)

    def _push_text(value: Any) -> None:
        text = str(value or "").strip()
        if text and text not in texts:
            texts.append(text)

    entries: list[Mapping[str, Any]] = []
    if isinstance(classification, Mapping):
        entries = [classification]
    elif isinstance(classification, Sequence):
        entries = [item for item in classification if isinstance(item, Mapping)]

    entries = sorted(entries, key=_entry_sort_key, reverse=True)
    for entry in entries:
        _push_cat_id(entry.get("@catId") or entry.get("@classId") or entry.get("@code"))
        _push_text(entry.get("#text") or entry.get("text"))

    if category_path:
        for part in str(category_path).split(">"):
            _push_text(part)
    if general_comment:
        _push_text(general_comment)

    return cat_ids, texts


def _entry_sort_key(entry: Mapping[str, Any]) -> tuple[int, int]:
    level_raw = str(entry.get("@level") or "").strip()
    cat_id = str(entry.get("@catId") or entry.get("@classId") or "").strip()
    try:
        level_value = int(level_raw)
    except ValueError:
        level_value = -1
    depth = cat_id.count(".")
    return (level_value, depth)


@lru_cache(maxsize=1)
def _load_entries(repository: TidasSchemaRepository) -> dict[str, ElementaryFlowClassificationEntry]:
    schema = repository.load(_SCHEMA_FILENAME)
    entries: dict[str, ElementaryFlowClassificationEntry] = {}
    for candidate in schema.get("oneOf", []):
        props = candidate.get("properties") if isinstance(candidate, dict) else None
        if not isinstance(props, dict):
            continue
        level = _extract_const(props.get("@level"))
        cat_id = _extract_const(props.get("@catId")) or _extract_const(props.get("@classId")) or _extract_const(props.get("@code"))
        text = _extract_const(props.get("#text"))
        if not level or not cat_id or not text:
            continue
        entries[cat_id] = ElementaryFlowClassificationEntry(level=level, cat_id=cat_id, text=text)
    return entries


def _extract_const(node: Any) -> str | None:
    if not isinstance(node, Mapping):
        return None
    value = node.get("const")
    if isinstance(value, str):
        return value
    return None


__all__ = ["ElementaryFlowClassificationRegistry", "infer_elementary_kind_and_compartment"]
