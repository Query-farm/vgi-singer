"""Singer message helpers: BATCH manifests, and the raw message row shape.

A BATCH message (Meltano SDK) points at files instead of carrying records::

    {"type": "BATCH", "stream": "users",
     "encoding": {"format": "jsonl", "compression": "gzip"},
     "manifest": ["file:///tmp/users-0001.json.gz", ...]}

Local ``jsonl`` manifests (plain or gzip) are expanded into records here, read
line by line so a large batch file is never loaded whole.
"""

from __future__ import annotations

import gzip
import io
import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

import pyarrow as pa

from .jsonschema import JSON_TYPE, TIMESTAMP_TYPE, Coercer
from .meta import column

URL_TIMEOUT_SECONDS = 60

MESSAGE_FIELDS: list[pa.Field[Any]] = [
    column("seq", pa.int64(), "Position of the message in the tap's output, from 1.", nullable=False),
    column("type", pa.string(), "Message type: SCHEMA, RECORD, STATE, ACTIVATE_VERSION or BATCH.", nullable=False),
    column("stream", pa.string(), "Stream the message belongs to; NULL for STATE."),
    column("record", JSON_TYPE, "RECORD: the record object."),
    column("time_extracted", TIMESTAMP_TYPE, "RECORD: when the tap extracted it, if sent."),
    column("version", pa.int64(), "RECORD / ACTIVATE_VERSION: the table version, if sent."),
    column("value", JSON_TYPE, "STATE: the state object (bookmarks)."),
    column("schema", JSON_TYPE, "SCHEMA: the stream's JSON Schema."),
    column("key_properties", pa.list_(pa.string()), "SCHEMA: the primary key properties."),
    column("bookmark_properties", pa.list_(pa.string()), "SCHEMA: the replication key properties."),
    column("manifest", pa.list_(pa.string()), "BATCH: the batch file URLs."),
    column("message", JSON_TYPE, "The whole message as JSON.", nullable=False),
]
MESSAGE_SCHEMA = pa.schema(MESSAGE_FIELDS)

FILE_MESSAGE_SCHEMA = pa.schema(
    [
        column("filename", pa.string(), "File the message was read from.", nullable=False),
        column("line", pa.int64(), "Line number of the message in the file, from 1.", nullable=False),
        *MESSAGE_FIELDS[1:],
    ]
)


def _str_list(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    return [str(v) for v in value]


def message_row(msg: dict[str, Any]) -> dict[str, Any]:
    """Return decoded columns (as JSON-able Python values) for one message."""
    kind = msg.get("type")
    return {
        "type": str(kind),
        "stream": msg.get("stream"),
        "record": msg.get("record") if kind == "RECORD" else None,
        "time_extracted": msg.get("time_extracted"),
        "version": msg.get("version"),
        "value": msg.get("value") if kind == "STATE" else None,
        "schema": msg.get("schema") if kind == "SCHEMA" else None,
        "key_properties": _str_list(msg.get("key_properties")),
        "bookmark_properties": _str_list(msg.get("bookmark_properties")),
        "manifest": _str_list(msg.get("manifest")),
        "message": msg,
    }


def message_batch(rows: list[dict[str, Any]], schema: pa.Schema, coercer: Coercer) -> pa.RecordBatch:
    """Return a record batch of message rows; JSON columns are serialized, others coerced."""
    arrays = []
    for field in schema:
        conv = coercer.converter(field.type)
        values = [conv(r.get(field.name)) if r.get(field.name) is not None else None for r in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _local_path(url: str) -> Path:
    parsed = urllib.parse.urlparse(url)
    netloc = parsed.netloc
    if parsed.scheme == "file" and len(netloc) >= 2 and netloc[0].isalpha() and netloc[1] == ":":
        # "file://" + a Windows path (file://C:\dir\f.gz): the drive lands in the netloc.
        return Path(urllib.parse.unquote(url[len("file://") :]))
    if parsed.scheme in ("", "file"):
        return Path(urllib.request.url2pathname(parsed.path) if parsed.scheme else url)
    raise ValueError(f"BATCH file {url!r} is not a local file; only file:// manifests are supported")


def open_text(source: Path | str) -> IO[str]:
    """Open a local file or http(s) URL as text, gunzipping it when it is gzip-compressed."""
    name = str(source)
    raw: IO[bytes]
    if name.startswith(("http://", "https://")):
        request = urllib.request.Request(name, headers={"User-Agent": "vgi-singer-worker/0.1"})
        try:
            response = urllib.request.urlopen(request, timeout=URL_TIMEOUT_SECONDS)
        except urllib.error.HTTPError as exc:
            raise ValueError(f"GET {name} failed with HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ValueError(f"GET {name} failed: {exc.reason}") from exc
        raw = io.BufferedReader(response)
    else:
        raw = open(name, "rb")  # noqa: SIM115 - the caller closes the returned wrapper
    peek = raw.peek(2) if isinstance(raw, io.BufferedReader) else b""
    if peek[:2] == b"\x1f\x8b":
        return io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8")
    return io.TextIOWrapper(raw, encoding="utf-8")


def batch_records(msg: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Return the records a BATCH message points at."""
    encoding = msg.get("encoding") or {}
    fmt = encoding.get("format", "jsonl") if isinstance(encoding, dict) else "jsonl"
    if fmt != "jsonl":
        raise ValueError(f"BATCH format {fmt!r} is not supported (only jsonl)")
    for url in msg.get("manifest") or []:
        with open_text(_local_path(str(url))) as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    if isinstance(record, dict):
                        yield record
