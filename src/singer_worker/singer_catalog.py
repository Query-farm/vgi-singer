"""Read Singer catalogs and build the per-scan catalog that selects one stream.

A Singer catalog lists ``streams``; each has a ``tap_stream_id``, a ``stream``
name, a JSON ``schema``, and ``metadata`` entries keyed by ``breadcrumb``
(``[]`` for the stream, ``["properties", name]`` for a property). A tap syncs
the streams whose stream-level metadata says ``selected: true`` and, within
them, the properties that are selected or ``inclusion: automatic``.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import pyarrow as pa

from .jsonschema import arrow_fields, stream_properties


def streams(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the stream entries of a catalog."""
    return [s for s in catalog.get("streams") or [] if isinstance(s, dict)]


def stream_id(stream: dict[str, Any]) -> str:
    """Return the stream's ``tap_stream_id`` (falling back to its name)."""
    return str(stream.get("tap_stream_id") or stream.get("stream") or "")


def stream_name(stream: dict[str, Any]) -> str:
    """Return the stream's ``stream`` name (falling back to its id)."""
    return str(stream.get("stream") or stream.get("tap_stream_id") or "")


def find_stream(catalog: dict[str, Any], name: str) -> dict[str, Any]:
    """Return the stream whose id or name is ``name`` (case-insensitive)."""
    wanted = name.lower()
    for key in (stream_id, stream_name):
        for s in streams(catalog):
            if key(s).lower() == wanted:
                return s
    known = ", ".join(sorted(stream_id(s) for s in streams(catalog))) or "none"
    raise ValueError(f"the tap has no stream named {name!r}; streams: {known}")


def table_names(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Table name -> stream: the stream name, or its id when names collide."""
    by_name: dict[str, list[dict[str, Any]]] = {}
    for s in streams(catalog):
        by_name.setdefault(stream_name(s).lower(), []).append(s)
    out: dict[str, dict[str, Any]] = {}
    for group in by_name.values():
        for s in group:
            out[stream_name(s) if len(group) == 1 else stream_id(s)] = s
    return out


def _metadata(stream: dict[str, Any]) -> dict[tuple[str, ...], dict[str, Any]]:
    out: dict[tuple[str, ...], dict[str, Any]] = {}
    for entry in stream.get("metadata") or []:
        if isinstance(entry, dict) and isinstance(entry.get("metadata"), dict):
            out[tuple(str(b) for b in entry.get("breadcrumb") or [])] = entry["metadata"]
    return out


def stream_info(stream: dict[str, Any]) -> dict[str, Any]:
    """Key properties, replication method/key and selection, from metadata or legacy top-level keys."""
    root = _metadata(stream).get((), {})
    key_props = root.get("table-key-properties", stream.get("key_properties")) or []
    return {
        "key_properties": [str(k) for k in key_props],
        "replication_method": root.get("forced-replication-method") or stream.get("replication_method"),
        "replication_key": root.get("replication-key") or stream.get("replication_key"),
        "valid_replication_keys": [str(k) for k in root.get("valid-replication-keys") or []],
        "selected": root.get("selected", stream.get("selected")),
    }


def unsupported(stream: dict[str, Any]) -> set[str]:
    """Properties the tap marks ``inclusion: unsupported`` (never emitted)."""
    return {
        crumb[1]
        for crumb, meta in _metadata(stream).items()
        if len(crumb) == 2 and crumb[0] == "properties" and meta.get("inclusion") == "unsupported"
    }


def automatic(stream: dict[str, Any]) -> set[str]:
    """Properties the tap always emits (``inclusion: automatic``), plus key properties."""
    out = {
        crumb[1]
        for crumb, meta in _metadata(stream).items()
        if len(crumb) == 2 and crumb[0] == "properties" and meta.get("inclusion") == "automatic"
    }
    return out | set(stream_info(stream)["key_properties"])


def stream_schema(stream: dict[str, Any]) -> pa.Schema:
    """Return the Arrow schema of a stream's table: one column per supported top-level property."""
    schema = stream.get("schema") if isinstance(stream.get("schema"), dict) else {}
    assert isinstance(schema, dict)
    skip = unsupported(stream)
    fields = [f for f in arrow_fields(schema) if f.name not in skip]
    return pa.schema(fields)


def select(stream: dict[str, Any], columns: list[str] | None) -> dict[str, Any]:
    """Return a one-stream catalog selecting ``stream`` and, if given, only ``columns`` (plus automatic ones).

    Both the metadata form (``selected`` on breadcrumbs) and the legacy form
    (``selected`` on the stream and its schema) are set, for older taps.
    """
    entry = copy.deepcopy(stream)
    props = list(stream_properties(entry.get("schema") or {}))
    keep = set(props) if columns is None else set(columns) | automatic(entry)
    meta = _metadata(entry)
    meta.setdefault((), {})["selected"] = True
    for name in props:
        meta.setdefault(("properties", name), {})["selected"] = name in keep
    entry["metadata"] = [{"breadcrumb": list(crumb), "metadata": m} for crumb, m in meta.items()]
    entry["selected"] = True
    if isinstance(entry.get("schema"), dict):
        entry["schema"]["selected"] = True
    return {"streams": [entry]}


def load_catalog(text: str) -> dict[str, Any]:
    """Parse catalog JSON text (raises ValueError with a clear message)."""
    try:
        catalog = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"the catalog is not valid JSON: {exc}") from exc
    if not isinstance(catalog, dict) or not isinstance(catalog.get("streams"), list):
        raise ValueError("a Singer catalog is a JSON object with a 'streams' list")
    return catalog
