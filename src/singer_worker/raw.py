"""Raw Singer messages: ``messages`` (from a running tap) and the file readers.

* ``messages(tap, ...)`` runs a tap and returns every message it prints, one
  row per message, for debugging a tap.
* ``read_messages(path)`` reads Singer JSONL output that is already on disk
  (``tap-foo > out.jsonl``; ``.gz`` too; ``path`` may be a glob).
* ``read_records(path, stream)`` reads the RECORD messages of one stream from
  such files as a typed table, using the stream's SCHEMA message in the file.
"""

from __future__ import annotations

import glob
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Annotated, Any

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
from .common import TapArgs, TapCursor, bind_config, full_catalog, parse_json_arg, process_config, read_items
from .jsonschema import Coercer, arrow_fields, records_to_batch
from .messages import FILE_MESSAGE_SCHEMA, MESSAGE_SCHEMA, batch_records, message_batch, message_row, open_text
from .meta import DYNAMIC_COLUMNS_MD, FIXTURE_URL, object_tags, result_columns
from .tap import discover, parse_message

FILE_BATCH_LINES = 5_000


@dataclass(slots=True, frozen=True, kw_only=True)
class MessagesArgs(TapArgs):
    """Arguments for ``messages(tap, stream := '', state := '', catalog_json := '')``."""

    stream: Annotated[str, Arg("stream", default="", doc="Select only this stream; empty selects every stream.")]
    state: Annotated[str, Arg("state", default="", doc="Singer state JSON to start from; empty = a full sync.")]
    catalog_json: Annotated[
        str, Arg("catalog_json", default="", doc="A Singer catalog JSON to use instead of running --discover.")
    ]


@dataclass(kw_only=True)
class MessagesCursor(TapCursor):
    """The tap cursor plus the number of messages returned so far."""

    emitted: int = 0


def _all_messages(msg: dict[str, Any]) -> list[dict[str, Any]]:
    return [msg]


@init_single_worker
class Messages(TableFunctionGenerator[MessagesArgs, MessagesCursor]):
    """Run a tap and return every Singer message it prints."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "messages"
        description = "Run a Singer tap and return its raw messages, one row per message"
        tags = {
            **object_tags(
                title="Raw Tap Messages",
                category="debugging",
                keywords="singer, messages, debug, schema, record, state, activate_version, batch, jsonl",
                doc_llm=(
                    "Run a Singer tap and return every message it prints on stdout as a row: seq, type "
                    "(SCHEMA/RECORD/STATE/ACTIVATE_VERSION/BATCH), stream, record (JSON), time_extracted, "
                    "version, value (STATE JSON), schema (JSON), key_properties, bookmark_properties, "
                    "manifest and message (the whole message JSON). For debugging taps and inspecting state; "
                    "use read_stream for typed rows. Named args: stream (select one stream; default all), "
                    "state, catalog_json, plus pip_url/executable/python/secret_name/timeout_seconds."
                ),
                doc_md=(
                    "## messages(tap, stream := '', state := '')\n\n"
                    "Everything a tap prints, one row per Singer message, for debugging taps and state."
                ),
            ),
            "vgi.result_columns_schema": result_columns(MESSAGE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql="SELECT type, count(*) AS n FROM singer.main.messages('tap-github', stream := 'issues') "
                "GROUP BY type ORDER BY type",
                description="Message counts by type for one tap-github stream",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[MessagesArgs]) -> BindResponse:
        """Resolve the secret and validate the JSON arguments."""
        if bind_config(params.secrets, params.args) is not None:
            if params.args.state:
                parse_json_arg(params.args.state, "state")
            if params.args.catalog_json:
                sc.load_catalog(params.args.catalog_json)
        return BindResponse(output_schema=MESSAGE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[MessagesArgs]) -> MessagesCursor:
        """Start from the given Singer state (or none)."""
        return MessagesCursor(singer_state=params.args.state)

    @classmethod
    def process(cls, params: ProcessParams[MessagesArgs], state: MessagesCursor, out: OutputCollector) -> None:
        """Read the next messages and emit them."""
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
        selected = sc.select(sc.find_stream(catalog, args.stream), None) if args.stream else full_catalog(catalog)
        msgs = read_items(
            state,
            spec=args.spec(),
            config=config,
            catalog=selected,
            timeout_seconds=args.timeout_seconds,
            items_of=_all_messages,
        )
        if not msgs:
            out.finish()
            return
        rows = []
        for msg in msgs:
            state.emitted += 1
            rows.append({"seq": state.emitted, **message_row(msg)})
        out.emit(message_batch(rows, params.output_schema, Coercer()))


# ------------------------------------------------------------------ file readers


def expand(path: str) -> list[str]:
    """Return the files a path or glob names, sorted (an http(s) URL is returned as is)."""
    if path.startswith(("http://", "https://")):
        return [path]
    files = sorted(glob.glob(str(Path(path).expanduser()))) if any(c in path for c in "*?[") else [path]
    files = [f for f in files if Path(f).is_file()]
    if not files:
        raise ValueError(f"no Singer output file matches {path!r}")
    return files


@dataclass(kw_only=True)
class FileCursor(ArrowSerializableDataclass):
    """Externalized position in the files: which file, and how many lines of it were read."""

    token: str = ""
    file_index: int = 0
    line: int = 0


_open: dict[str, tuple[int, int, IO[str]]] = {}
_open_lock = threading.Lock()


def _reader(cursor: FileCursor, files: list[str]) -> IO[str]:
    """Return an open handle positioned after ``cursor.line`` lines of the current file."""
    with _open_lock:
        hit = _open.pop(cursor.token, None)
    if hit is not None:
        index, line, handle = hit
        if (index, line) == (cursor.file_index, cursor.line):
            return handle
        handle.close()
    handle = open_text(files[cursor.file_index])
    for _ in range(cursor.line):
        if not handle.readline():
            break
    return handle


def read_file_lines(cursor: FileCursor, files: list[str], limit: int) -> list[tuple[str, int, dict[str, Any]]]:
    """Return the next ``limit`` messages as (filename, line number, message); empty at the end."""
    if not cursor.token:
        cursor.token = uuid.uuid4().hex
    out: list[tuple[str, int, dict[str, Any]]] = []
    while cursor.file_index < len(files) and len(out) < limit:
        handle = _reader(cursor, files)
        while len(out) < limit:
            text = handle.readline()
            if not text:
                break
            cursor.line += 1
            msg = parse_message(text)
            if msg is not None:
                out.append((files[cursor.file_index], cursor.line, msg))
        if len(out) >= limit:
            with _open_lock:
                _open[cursor.token] = (cursor.file_index, cursor.line, handle)
            break
        handle.close()
        cursor.file_index += 1
        cursor.line = 0
    return out


@dataclass(slots=True, frozen=True, kw_only=True)
class PathArgs:
    """Arguments for ``read_messages(path)``."""

    path: Annotated[str, Arg(0, doc="Singer JSONL output file (plain or .gz), or a glob such as 'out/*.jsonl'.")]


@init_single_worker
class ReadMessages(TableFunctionGenerator[PathArgs, FileCursor]):
    """Read Singer messages from JSONL files on disk."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "read_messages"
        description = "Read Singer JSONL output files as raw messages, one row per message"
        tags = {
            **object_tags(
                title="Read Singer Output Files",
                category="files",
                keywords="singer, jsonl, file, output, messages, replay, gzip, glob",
                doc_llm=(
                    "Read Singer messages that a tap already wrote to disk (`tap-foo ... > out.jsonl`, plain or "
                    "gzip; `path` may be a glob or an http(s) URL) and return one row per message: filename, "
                    "line, type, stream, record (JSON), time_extracted, version, value (STATE JSON), schema (JSON), "
                    "key_properties, "
                    "bookmark_properties, manifest and message. No tap is run and no secret is needed. Lines "
                    "that are not JSON messages are skipped."
                ),
                doc_md=(
                    "## read_messages(path)\n\n"
                    "Replay saved Singer output (a file, glob or http(s) URL; plain or gzip), one row per message."
                ),
            ),
            "vgi.result_columns_schema": result_columns(FILE_MESSAGE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=f"SELECT type, count(*) AS n FROM singer.main.read_messages('{FIXTURE_URL}') "
                "GROUP BY type ORDER BY type",
                description="Message counts by type in saved Singer output",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PathArgs]) -> BindResponse:
        """Check that the files exist."""
        expand(params.args.path)
        return BindResponse(output_schema=FILE_MESSAGE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[PathArgs]) -> FileCursor:
        """Start at the first line of the first file."""
        return FileCursor()

    @classmethod
    def process(cls, params: ProcessParams[PathArgs], state: FileCursor, out: OutputCollector) -> None:
        """Emit the next lines' messages."""
        items = read_file_lines(state, expand(params.args.path), FILE_BATCH_LINES)
        if not items:
            out.finish()
            return
        rows = [{"filename": f, "line": n, **message_row(m)} for f, n, m in items]
        out.emit(message_batch(rows, params.output_schema, Coercer()))


@dataclass(slots=True, frozen=True, kw_only=True)
class RecordsArgs:
    """Arguments for ``read_records(path, stream)``."""

    path: Annotated[str, Arg(0, doc="Singer JSONL output file (plain or .gz), or a glob.")]
    stream: Annotated[str, Arg(1, doc="Stream whose RECORD messages to return.")]


def _file_schema(files: list[str], stream: str) -> dict[str, Any]:
    for name in files:
        with open_text(name) as handle:
            for text in handle:
                msg = parse_message(text)
                if msg and msg["type"] == "SCHEMA" and msg.get("stream") == stream:
                    schema = msg.get("schema")
                    if isinstance(schema, dict):
                        return schema
    raise ValueError(f"no SCHEMA message for stream {stream!r} in {', '.join(files)}")


@init_single_worker
class ReadRecords(TableFunctionGenerator[RecordsArgs, FileCursor]):
    """Read one stream's records from Singer JSONL files as a typed table."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "read_records"
        description = "Read one stream's records from Singer JSONL output files as a typed table"
        tags = {
            **object_tags(
                title="Read Singer Records From Files",
                category="files",
                keywords="singer, jsonl, records, file, typed, schema, replay",
                doc_llm=(
                    "Read the RECORD (and local BATCH) messages of `stream` from Singer JSONL files (`path`, "
                    "plain/gzip/glob/http(s) URL) and return them as typed columns derived from the stream's "
                    "first SCHEMA message in the files, using the same JSON Schema mapping as read_stream "
                    "(date-time -> `TIMESTAMPTZ`, objects -> `STRUCT`, arrays -> `LIST`, open shapes -> `JSON`)."
                ),
                doc_md=(
                    "## read_records(path, stream)\n\n"
                    "Typed rows of one stream from saved Singer output, typed from its SCHEMA message."
                ),
            ),
            "vgi.result_dynamic_columns_md": DYNAMIC_COLUMNS_MD,
        }
        examples = [
            FunctionExample(
                sql=f"SELECT count(*) AS events FROM singer.main.read_records('{FIXTURE_URL}', 'events')",
                description="Count the events records in saved Singer output",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[RecordsArgs]) -> BindResponse:
        """Derive the columns from the stream's SCHEMA message."""
        schema = pa.schema(arrow_fields(_file_schema(expand(params.args.path), params.args.stream)))
        if not len(schema):
            raise ValueError(f"stream {params.args.stream!r} declares no properties")
        return BindResponse(output_schema=schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[RecordsArgs]) -> FileCursor:
        """Start at the first line of the first file."""
        return FileCursor()

    @classmethod
    def process(cls, params: ProcessParams[RecordsArgs], state: FileCursor, out: OutputCollector) -> None:
        """Emit the stream's records from the next lines."""
        files = expand(params.args.path)
        while True:
            items = read_file_lines(state, files, FILE_BATCH_LINES)
            if not items:
                out.finish()
                return
            records: list[dict[str, Any]] = []
            for _, _, msg in items:
                if msg.get("stream") != params.args.stream:
                    continue
                if msg["type"] == "RECORD" and isinstance(msg.get("record"), dict):
                    records.append(msg["record"])
                elif msg["type"] == "BATCH":
                    records.extend(batch_records(msg))
            if records:
                out.emit(records_to_batch(records, params.output_schema, Coercer()))
                return
