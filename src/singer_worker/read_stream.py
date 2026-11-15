"""``read_stream``: run a tap and return one stream as a typed table, and ``final_state``.

Bind runs ``tap --discover`` (or reads the ``catalog_json`` argument) to find
the stream and turn its JSON Schema into columns. The scan then runs the tap
with a catalog that selects only that stream and, thanks to projection
pushdown, only the columns the query uses (plus the tap's automatic and key
properties). Rows stream back in batches of up to 1,000 as the tap prints
them; see ``common.py`` for how the scan resumes from Singer STATE.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
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
from .common import (
    TapArgs,
    TapCursor,
    bind_config,
    full_catalog,
    parse_json_arg,
    process_config,
    read_items,
)
from .jsonschema import JSON_TYPE, Coercer, records_to_batch
from .messages import batch_records
from .meta import DYNAMIC_COLUMNS_MD, column, object_tags, result_columns
from .tap import TapRun, discover

log = logging.getLogger(__name__)

_PLACEHOLDER = pa.schema([pa.field("pending", pa.string())])


@dataclass(slots=True, frozen=True, kw_only=True)
class ReadStreamArgs(TapArgs):
    """Arguments for ``read_stream(tap, stream, ...)``."""

    stream: Annotated[str, Arg(1, doc="Stream to read: its tap_stream_id or stream name.")]
    state: Annotated[
        str,
        Arg("state", default="", doc="Singer state JSON to start from (bookmarks); empty = a full sync."),
    ]
    catalog_json: Annotated[
        str,
        Arg(
            "catalog_json",
            default="",
            doc="A Singer catalog JSON to use instead of running --discover (e.g. from singer.main.catalog).",
        ),
    ]


def _records_of(stream: dict[str, Any]) -> Any:
    names = {sc.stream_id(stream), sc.stream_name(stream)}

    def items_of(msg: dict[str, Any]) -> list[dict[str, Any]]:
        if msg.get("stream") not in names:
            return []
        if msg["type"] == "RECORD" and isinstance(msg.get("record"), dict):
            return [msg["record"]]
        if msg["type"] == "BATCH":
            return list(batch_records(msg))
        return []

    return items_of


def _bound_stream(params: BindParams[Any], config: str) -> dict[str, Any]:
    args = params.args
    if args.catalog_json:
        catalog = sc.load_catalog(args.catalog_json)
    else:
        catalog = discover(args.spec(), config, args.timeout_seconds)
    return sc.find_stream(catalog, args.stream)


@init_single_worker
class ReadStream(TableFunctionGenerator[ReadStreamArgs, TapCursor]):
    """Run a Singer tap and stream one of its streams as typed rows."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "read_stream"
        description = "Run a Singer tap and return one stream as a typed table"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Read Singer Stream",
                category="taps",
                keywords="singer, tap, meltano, stream, sync, extract, elt, records",
                doc_llm=(
                    "Run a Singer tap and return one stream as a table. `tap` is the tap executable name and "
                    "`stream` the tap_stream_id or stream name. Columns come from the stream's JSON Schema: "
                    "integer -> `BIGINT`, number -> `DOUBLE`, boolean -> `BOOLEAN`, string -> `VARCHAR`, "
                    "date-time -> `TIMESTAMPTZ`, date -> `DATE`, objects -> `STRUCT`, arrays -> `LIST`, open "
                    "shapes -> `JSON`. The config comes from a `singer` secret scoped to 'singer://<tap>'. "
                    "Named args: pip_url (pip requirement; installed once into an isolated cached venv), "
                    "executable (an installed tap), python, secret_name, timeout_seconds, state (Singer state "
                    "JSON to start an incremental sync from) and catalog_json (skip --discover). Only the "
                    "projected columns are selected in the tap's catalog."
                ),
                doc_md=(
                    "## read_stream(tap, stream, ...)\n\n"
                    "One stream of a Singer tap as a typed table.\n\n"
                    "The tap is installed on first use into its own cached virtual environment from "
                    "`pip_url`, and its config is read from a `singer` secret."
                ),
            ),
            "vgi.result_dynamic_columns_md": DYNAMIC_COLUMNS_MD,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT id, name FROM singer.main.read_stream('tap-csv', 'customers', "
                    "pip_url := 'git+https://github.com/MeltanoLabs/tap-csv.git') ORDER BY id LIMIT 10"
                ),
                description="The first ten customers from a CSV file read by MeltanoLabs tap-csv",
            ),
            FunctionExample(
                sql=(
                    "SELECT count(*) AS commits FROM singer.main.read_stream('tap-github', 'commits', "
                    "state := '{\"bookmarks\": {}}')"
                ),
                description="Count the commits tap-github syncs, starting from an empty state",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ReadStreamArgs]) -> BindResponse:
        """Discover the stream and publish its columns."""
        config = bind_config(params.secrets, params.args)
        if config is None:
            return BindResponse(output_schema=_PLACEHOLDER)
        if params.args.state:
            parse_json_arg(params.args.state, "state")
        stream = _bound_stream(params, config)
        schema = sc.stream_schema(stream)
        if not len(schema):
            raise ValueError(f"stream {params.args.stream!r} declares no properties, so it has no columns")
        return BindResponse(output_schema=schema, opaque_data=json.dumps(stream).encode())

    @classmethod
    def initial_state(cls, params: ProcessParams[ReadStreamArgs]) -> TapCursor:
        """Start from the given Singer state (or none)."""
        return TapCursor(singer_state=params.args.state)

    @classmethod
    def process(cls, params: ProcessParams[ReadStreamArgs], state: TapCursor, out: OutputCollector) -> None:
        """Read the next records of the stream and emit them as one batch."""
        if state.done:
            out.finish()
            return
        assert params.init_call is not None and params.init_call.bind_opaque_data
        stream = json.loads(params.init_call.bind_opaque_data)
        coercer = Coercer()
        records = read_items(
            state,
            spec=params.args.spec(),
            config=process_config(params.secrets, params.args),
            catalog=sc.select(stream, list(params.output_schema.names)),
            timeout_seconds=params.args.timeout_seconds,
            items_of=_records_of(stream),
        )
        if records:
            out.emit(records_to_batch(records, params.output_schema, coercer))
        elif state.done:
            out.finish()
            return
        if coercer.rejected:
            log.warning("singer: %d values did not match their declared type and were set to NULL", coercer.rejected)


# ---------------------------------------------------------------- final_state


@dataclass(slots=True, frozen=True, kw_only=True)
class FinalStateArgs(TapArgs):
    """Arguments for ``final_state(tap, stream := '', state := '')``."""

    stream: Annotated[str, Arg("stream", default="", doc="Only sync this stream; empty syncs every stream.")]
    state: Annotated[str, Arg("state", default="", doc="Singer state JSON to start from; empty = a full sync.")]
    catalog_json: Annotated[
        str, Arg("catalog_json", default="", doc="A Singer catalog JSON to use instead of running --discover.")
    ]


@dataclass(kw_only=True)
class DoneState(ArrowSerializableDataclass):
    """Cursor: whether the single result row was produced."""

    done: bool = False


FINAL_STATE_SCHEMA = pa.schema(
    [
        column("final_state", JSON_TYPE, "The last STATE value the tap emitted (NULL if it emitted none)."),
        column("records", pa.int64(), "RECORD messages read (plus records in BATCH files).", nullable=False),
        column("state_messages", pa.int64(), "STATE messages read.", nullable=False),
        column("streams", pa.list_(pa.string()), "Streams that produced records, sorted.", nullable=False),
    ]
)


@init_single_worker
class FinalState(TableFunctionGenerator[FinalStateArgs, DoneState]):
    """Sync a tap from a state and return the state it ends with."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "final_state"
        description = "Run a tap from a Singer state and return the final state and record counts"
        tags = {
            **object_tags(
                title="Final Singer State",
                category="taps",
                keywords="singer, state, bookmark, incremental, sync, resume, checkpoint",
                doc_llm=(
                    "Run a Singer tap to completion, starting from the `state` JSON (named arg, empty = full "
                    "sync), and return one row: `final_state` (JSON, the last STATE the tap emitted - pass it "
                    "as `state` next time for an incremental sync), `records`, `state_messages` and "
                    "`streams`. `stream :=` limits the sync to one stream. Records are counted, not "
                    "returned; use read_stream for the rows. Same tap/pip_url/secret arguments as read_stream."
                ),
                doc_md=(
                    "## final_state(tap, stream := '', state := '')\n\n"
                    "Sync a tap and keep only the final Singer state, for incremental pipelines: pass the "
                    "returned `final_state` as `state` on the next run."
                ),
            ),
            "vgi.result_columns_schema": result_columns(FINAL_STATE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT records, final_state->'$.bookmarks' AS bookmarks "
                    "FROM singer.main.final_state('tap-github', stream := 'issues')"
                ),
                description="Sync one tap-github stream and keep the bookmarks to resume from",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FinalStateArgs]) -> BindResponse:
        """Resolve the secret and validate the JSON arguments."""
        config = bind_config(params.secrets, params.args)
        if config is not None:
            if params.args.state:
                parse_json_arg(params.args.state, "state")
            if params.args.catalog_json:
                sc.load_catalog(params.args.catalog_json)
        return BindResponse(output_schema=FINAL_STATE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FinalStateArgs]) -> DoneState:
        """Not started."""
        return DoneState()

    @classmethod
    def process(cls, params: ProcessParams[FinalStateArgs], state: DoneState, out: OutputCollector) -> None:
        """Run the tap to the end, counting records, and emit the one result row."""
        if state.done:
            out.finish()
            return
        args = params.args
        config = process_config(params.secrets, args)
        catalog = (
            sc.load_catalog(args.catalog_json)
            if args.catalog_json
            else discover(args.spec(), config, args.timeout_seconds)
        )
        catalog = sc.select(sc.find_stream(catalog, args.stream), None) if args.stream else full_catalog(catalog)
        run = TapRun.start(args.spec(), config, catalog, args.state or None, timeout_seconds=args.timeout_seconds)
        records = states = 0
        final: Any = None
        seen: set[str] = set()
        try:
            for msg in run.messages():
                kind = msg["type"]
                if kind == "RECORD":
                    records += 1
                    seen.add(str(msg.get("stream")))
                elif kind == "BATCH":
                    records += sum(1 for _ in batch_records(msg))
                    seen.add(str(msg.get("stream")))
                elif kind == "STATE":
                    states += 1
                    final = msg.get("value")
        finally:
            run.close()
        row = {
            "final_state": None if final is None else json.dumps(final, separators=(",", ":")),
            "records": records,
            "state_messages": states,
            "streams": sorted(seen),
        }
        out.emit(pa.RecordBatch.from_pylist([row], schema=params.output_schema))
        state.done = True
