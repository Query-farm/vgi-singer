"""``discover`` (one row per stream) and ``catalog`` (the raw catalog JSON) for a tap."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from . import singer_catalog as sc
from .common import TapArgs, bind_config, process_config
from .jsonschema import JSON_TYPE, duckdb_type_name
from .meta import column, object_tags, result_columns
from .tap import discover

COLUMN_TYPE = pa.struct([pa.field("name", pa.string()), pa.field("type", pa.string())])

DISCOVER_SCHEMA = pa.schema(
    [
        column("tap_stream_id", pa.string(), "The stream's unique id in the tap.", nullable=False),
        column("stream", pa.string(), "The stream name.", nullable=False),
        column("table_name", pa.string(), "Table name the stream gets in an attached tap catalog.", nullable=False),
        column("key_properties", pa.list_(pa.string()), "Primary key properties.", nullable=False),
        column("replication_method", pa.string(), "FULL_TABLE, INCREMENTAL or LOG_BASED, if declared."),
        column("replication_key", pa.string(), "Property incremental syncs bookmark on, if declared."),
        column("valid_replication_keys", pa.list_(pa.string()), "Properties that can be replication keys."),
        column("selected", pa.bool_(), "Whether the discovered catalog marks the stream selected."),
        column("columns", pa.list_(COLUMN_TYPE), "Table columns with their DuckDB types.", nullable=False),
        column("schema", JSON_TYPE, "The stream's JSON Schema.", nullable=False),
        column("metadata", JSON_TYPE, "The stream's Singer metadata entries."),
    ]
)

CATALOG_SCHEMA = pa.schema(
    [
        column("catalog", JSON_TYPE, "The whole catalog printed by --discover.", nullable=False),
        column("streams", pa.int64(), "Number of streams in the catalog.", nullable=False),
    ]
)


@dataclass(kw_only=True)
class OnceState(ArrowSerializableDataclass):
    """Cursor: whether the rows were produced."""

    done: bool = False


def _rows(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    tables = {sc.stream_id(s): name for name, s in sc.table_names(catalog).items()}
    rows = []
    for s in sc.streams(catalog):
        info = sc.stream_info(s)
        schema = sc.stream_schema(s)
        rows.append(
            {
                "tap_stream_id": sc.stream_id(s),
                "stream": sc.stream_name(s),
                "table_name": tables.get(sc.stream_id(s), sc.stream_name(s)),
                "key_properties": info["key_properties"],
                "replication_method": info["replication_method"],
                "replication_key": info["replication_key"],
                "valid_replication_keys": info["valid_replication_keys"],
                "selected": info["selected"] if isinstance(info["selected"], bool) else None,
                "columns": [{"name": f.name, "type": duckdb_type_name(f.type)} for f in schema],
                "schema": json.dumps(s.get("schema") or {}),
                "metadata": json.dumps(s["metadata"]) if "metadata" in s else None,
            }
        )
    return rows


_TAP_DOC = (
    "`tap` is the tap executable name; named args pip_url (pip requirement, installed once into an isolated "
    "cached venv), executable, python, secret_name and timeout_seconds as in read_stream. The config comes "
    "from a `singer` secret scoped to 'singer://<tap>'."
)


@init_single_worker
class Discover(TableFunctionGenerator[TapArgs, OnceState]):
    """Run ``tap --discover`` and list its streams."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "discover"
        description = "Run a Singer tap's discovery and list its streams with their columns"
        tags = {
            **object_tags(
                title="Discover Tap Streams",
                category="taps",
                keywords="singer, discover, catalog, streams, schema, tap, meltano",
                doc_llm=(
                    "Run `tap --discover` and return one row per stream: tap_stream_id, stream, table_name, "
                    "key_properties, replication_method, replication_key, valid_replication_keys, selected, "
                    "columns (a `LIST` of `STRUCT(name, type)` with DuckDB types), schema (JSON Schema) and "
                    "metadata (JSON). Use it to see what a tap offers before read_stream. " + _TAP_DOC
                ),
                doc_md=(
                    "## discover(tap, ...)\n\n"
                    "The streams a tap offers, with their key properties, replication settings and columns."
                ),
            ),
            "vgi.result_columns_schema": result_columns(DISCOVER_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT stream, key_properties, replication_method, len(columns) AS column_count "
                    "FROM singer.main.discover('tap-github') ORDER BY stream"
                ),
                description="Which streams tap-github offers and how each replicates",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TapArgs]) -> BindResponse:
        """Resolve the secret; the columns are fixed."""
        bind_config(params.secrets, params.args)
        return BindResponse(output_schema=DISCOVER_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[TapArgs]) -> OnceState:
        """Not started."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[TapArgs], state: OnceState, out: OutputCollector) -> None:
        """Discover and emit one row per stream."""
        if state.done:
            out.finish()
            return
        args = params.args
        catalog = discover(args.spec(), process_config(params.secrets, args), args.timeout_seconds)
        out.emit(pa.RecordBatch.from_pylist(_rows(catalog), schema=params.output_schema))
        state.done = True


@init_single_worker
class CatalogJson(TableFunctionGenerator[TapArgs, OnceState]):
    """Run ``tap --discover`` and return the catalog JSON."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "catalog"
        description = "Run a Singer tap's discovery and return the catalog JSON"
        tags = {
            **object_tags(
                title="Tap Catalog JSON",
                category="taps",
                keywords="singer, catalog, discover, json, properties, tap",
                doc_llm=(
                    "Run `tap --discover` and return one row: `catalog` (the whole Singer catalog as JSON) and "
                    "`streams` (count). Pass the catalog to read_stream(catalog_json := ...) to skip discovery, "
                    "or to ATTACH (catalog getvariable('...')) to expose every stream as a table. " + _TAP_DOC
                ),
                doc_md=(
                    "## catalog(tap, ...)\n\n"
                    "The raw Singer catalog, for example to attach a tap as a database with "
                    "`catalog getvariable('cat')` after storing it in a variable."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CATALOG_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT streams, json_keys(catalog) AS keys FROM singer.main.catalog('tap-github')",
                description="How many streams tap-github discovers",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TapArgs]) -> BindResponse:
        """Resolve the secret; the columns are fixed."""
        bind_config(params.secrets, params.args)
        return BindResponse(output_schema=CATALOG_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[TapArgs]) -> OnceState:
        """Not started."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[TapArgs], state: OnceState, out: OutputCollector) -> None:
        """Discover and emit the catalog."""
        if state.done:
            out.finish()
            return
        args = params.args
        catalog = discover(args.spec(), process_config(params.secrets, args), args.timeout_seconds)
        row = {"catalog": json.dumps(catalog), "streams": len(sc.streams(catalog))}
        out.emit(pa.RecordBatch.from_pylist([row], schema=params.output_schema))
        state.done = True
