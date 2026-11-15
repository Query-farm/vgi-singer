"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arguments
from vgi.client import Client

from singer_worker import singer_catalog as sc
from singer_worker import tap as tapmod
from singer_worker.catalog import tap_attach
from singer_worker.common import TapCursor, read_items
from singer_worker.jsonschema import JSON_TYPE, TIMESTAMP_TYPE, Coercer, arrow_type, duckdb_type_name, records_to_batch
from singer_worker.schema_type import schema_type_names
from singer_worker.tap import TapError, TapRun, TapSpec, discover, private_file

ROOT = Path(__file__).resolve().parent.parent
FAKE_TAP = str(ROOT / "test" / "fake_tap" / "tap-fake")
FIXTURE = ROOT / "test" / "fixtures" / "fake-output.jsonl"
SPEC = TapSpec(tap="tap-fake", executable=FAKE_TAP)
SCHEMA = ["main"]


# ------------------------------------------------------------ JSON Schema mapping


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ({"type": "integer"}, pa.int64()),
        ({"type": ["null", "integer"]}, pa.int64()),
        ({"type": ["integer", "number"]}, pa.float64()),
        ({"anyOf": [{"type": "boolean"}, {"type": "null"}]}, pa.bool_()),
        ({"type": "string", "format": "date-time"}, TIMESTAMP_TYPE),
        ({"anyOf": [{"type": "string", "format": "date"}, {"type": "null"}]}, pa.date32()),
        ({"type": "string", "format": "time"}, pa.time64("us")),
        ({"type": ["string", "integer"]}, JSON_TYPE),
        ({}, JSON_TYPE),
        ({"type": "object"}, JSON_TYPE),
        ({"type": "object", "properties": {"a": {"type": "integer"}}, "additionalProperties": True}, JSON_TYPE),
        (
            {"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "string"}}},
            pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.string())]),
        ),
        ({"type": "array", "items": {"type": "string"}}, pa.list_(pa.string())),
        ({"type": "array"}, pa.list_(JSON_TYPE)),
    ],
)
def test_arrow_type(schema: dict[str, Any], expected: pa.DataType) -> None:
    assert arrow_type(schema) == expected


def test_deep_objects_become_json() -> None:
    schema: dict[str, Any] = {"type": "integer"}
    for _ in range(6):
        schema = {"type": "object", "properties": {"x": schema}}
    inner = arrow_type(schema)
    depth = 0
    while pa.types.is_struct(inner):
        inner = inner.field("x").type
        depth += 1
    assert inner == JSON_TYPE
    assert depth == 4


def test_duckdb_type_names() -> None:
    assert duckdb_type_name(pa.list_(pa.struct([pa.field("a", pa.int64())]))) == 'STRUCT("a" BIGINT)[]'
    assert schema_type_names(['{"type": "number"}', None, "not json"]) == ["DOUBLE", None, None]


def test_coercion_and_rejects() -> None:
    schema = pa.schema(
        [  # type: ignore[arg-type]
            pa.field("i", pa.int64()),
            pa.field("ts", TIMESTAMP_TYPE),
            pa.field("d", pa.date32()),
            pa.field("s", pa.struct([pa.field("a", pa.int64())])),
            pa.field("j", JSON_TYPE),
            pa.field("b", pa.bool_()),
        ]
    )
    coercer = Coercer()
    batch = records_to_batch(
        [
            {
                "i": "42",
                "ts": "2026-01-02T02:00:00+02:00",
                "d": "2026-03-04",
                "s": {"a": 1, "z": 9},
                "j": [1],
                "b": "t",
            },
            {"i": "abc", "ts": "garbage", "d": "2026-03-04T10:00:00Z", "s": "nope", "j": None},
        ],
        schema,
        coercer,
    )
    rows = batch.to_pylist()
    assert rows[0]["i"] == 42
    assert rows[0]["ts"] == dt.datetime(2026, 1, 2, 0, 0, tzinfo=dt.UTC)
    assert rows[0]["s"] == {"a": 1}
    assert json.loads(rows[0]["j"]) == [1]
    assert rows[0]["b"] is True
    assert rows[1]["i"] is None and rows[1]["ts"] is None and rows[1]["s"] is None
    assert rows[1]["d"] == dt.date(2026, 3, 4)
    assert coercer.rejected == 3


# ------------------------------------------------------------------- catalogs


def _fake_catalog() -> dict[str, Any]:
    return sc.load_catalog((ROOT / "test" / "fixtures" / "fake-catalog.json").read_text())


def test_stream_schema_skips_unsupported() -> None:
    users = sc.find_stream(_fake_catalog(), "USERS")
    names = sc.stream_schema(users).names
    assert "secret_note" not in names
    assert names[:3] == ["id", "name", "email"]


def test_select_keeps_automatic_and_requested() -> None:
    users = sc.find_stream(_fake_catalog(), "users")
    selected = sc.select(users, ["name"])["streams"][0]
    flags = {tuple(m["breadcrumb"]): m["metadata"].get("selected") for m in selected["metadata"]}
    assert flags[()] is True
    assert flags[("properties", "id")] is True  # key property
    assert flags[("properties", "name")] is True
    assert flags[("properties", "email")] is False
    assert selected["selected"] is True
    assert users["metadata"][0]["metadata"].get("selected") is None  # original untouched


def test_table_names_disambiguate() -> None:
    catalog = {"streams": [{"tap_stream_id": "a-x", "stream": "x"}, {"tap_stream_id": "b-x", "stream": "x"}]}
    assert set(sc.table_names(catalog)) == {"a-x", "b-x"}


def test_attach_options() -> None:
    assert tap_attach({}) is None
    with pytest.raises(ValueError, match="needs a catalog"):
        tap_attach({"tap": "tap-fake"})
    path = str(ROOT / "test" / "fixtures" / "fake-catalog.json")
    attach = tap_attach({"tap": "tap-fake", "catalog": path, "timeout_seconds": 5})
    assert attach is not None and attach.timeout_seconds == 5
    assert len(sc.streams(attach.catalog)) == 3
    with pytest.raises(ValueError, match="neither catalog JSON nor"):
        tap_attach({"tap": "tap-fake", "catalog": "/nonexistent.json"})


# ----------------------------------------------------------------- tap process


def test_private_file_is_0600_and_removed() -> None:
    with private_file('{"a": 1}', ".json") as path:
        assert os.stat(path).st_mode & 0o777 == 0o600
    assert not os.path.exists(path)


def test_rejects_paths_as_tap_names() -> None:
    with pytest.raises(TapError, match="tap executable name"):
        TapSpec(tap="../evil")


def test_discover_fake_tap() -> None:
    catalog = discover(SPEC, "{}")
    assert [sc.stream_id(s) for s in sc.streams(catalog)] == ["users", "events", "batched"]


def _events(config: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    stream = sc.find_stream(discover(SPEC, json.dumps(config)), "events")
    return stream, sc.select(stream, None)


def _records(msg: dict[str, Any]) -> list[dict[str, Any]]:
    if msg["type"] == "RECORD" and msg.get("stream") == "events":
        return [msg["record"]]
    return []


def _scan(config: dict[str, Any], *, restart_every_batch: bool, start_state: str = "") -> list[int]:
    _, catalog = _events(config)
    cursor = TapCursor(singer_state=start_state)
    ids: list[int] = []
    batches = 0
    while not cursor.done:
        items = read_items(
            cursor,
            spec=SPEC,
            config=json.dumps(config),
            catalog=catalog,
            timeout_seconds=30,
            items_of=_records,
            max_items=333,
        )
        ids.extend(r["id"] for r in items)
        batches += 1
        # Round-trip the cursor through Arrow, as the HTTP transport does.
        cursor = TapCursor.deserialize_from_bytes(cursor.serialize_to_bytes())
        if restart_every_batch:
            # Another worker process: the live tap is gone, only the cursor remains.
            tapmod.drop_run(cursor.token)
    assert batches >= 3
    return ids


def test_scan_streams_in_batches() -> None:
    assert _scan({"events_rows": 1000}, restart_every_batch=False) == list(range(1, 1001))


def test_scan_resumes_from_state_in_another_process() -> None:
    # Restarting the tap from the cursor's last STATE and skipping what was already
    # returned must give exactly the same rows: no gaps, no duplicates.
    assert _scan({"events_rows": 1000, "state_every": 70}, restart_every_batch=True) == list(range(1, 1001))


def test_scan_without_any_state_messages_still_resumes() -> None:
    assert _scan({"events_rows": 700, "state_every": 100_000}, restart_every_batch=True)[-3:] == [698, 699, 700]


def test_scan_from_initial_state() -> None:
    state = json.dumps({"bookmarks": {"events": {"replication_key_value": 900}}})
    assert _scan({"events_rows": 2000}, restart_every_batch=True, start_state=state) == list(range(901, 2001))


def test_nonzero_exit_quotes_stderr() -> None:
    _, catalog = _events({})
    run = TapRun.start(SPEC, json.dumps({"fail_after": 5}), catalog, None)
    with pytest.raises(TapError, match=r"exited with code 3[\s\S]*upstream exploded"):
        list(run.messages())
    assert all(not os.path.exists(f) for f in run.files)


def test_silent_tap_times_out() -> None:
    _, catalog = _events({})
    run = TapRun.start(SPEC, json.dumps({"hang_after": 2}), catalog, None, timeout_seconds=1)
    with pytest.raises(TapError, match="printed nothing for 1 seconds"):
        list(run.messages())
    assert run.proc.poll() is not None


def test_installs_into_isolated_cached_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A local package installed by pip_url lands in the cache, not in the worker's env."""
    if shutil.which("uv") is None:
        pytest.skip("uv is not installed")
    pkg = tmp_path / "pkg"
    (pkg / "tap_local").mkdir(parents=True)
    (pkg / "tap_local" / "__init__.py").write_text("def main():\n    print('{}')\n")
    (pkg / "pyproject.toml").write_text(
        '[project]\nname = "tap-local"\nversion = "0.0.1"\n'
        '[project.scripts]\ntap-local = "tap_local:main"\n'
        '[build-system]\nrequires = ["setuptools"]\nbuild-backend = "setuptools.build_meta"\n'
    )
    monkeypatch.setenv(tapmod.CACHE_ENV_VAR, str(tmp_path / "cache"))
    monkeypatch.setenv("UV_OFFLINE", "1")
    spec = TapSpec(tap="tap-local", pip_url=str(pkg))
    try:
        exe = tapmod.resolve_executable(spec)
    except TapError as exc:
        pytest.skip(f"cannot build a local package offline here: {exc}")
    assert exe.startswith(str(tmp_path / "cache" / "taps"))
    assert tapmod.resolve_executable(spec) == exe  # cached
    assert shutil.which("tap-local") is None


# ------------------------------------------------------- file readers (in-process)


def _table(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments))
    return pa.Table.from_batches(batches)


def test_read_messages(client: Client) -> None:
    table = _table(client, "read_messages", str(FIXTURE))
    types = table.column("type").to_pylist()
    assert types.count("RECORD") == 253
    assert types.count("SCHEMA") == 2
    assert table.column("line").to_pylist()[0] == 2  # line 1 is a stray log line


def test_read_records_gzip_and_glob(client: Client, tmp_path: Path) -> None:
    for i in range(2):
        with gzip.open(tmp_path / f"part{i}.jsonl.gz", "wt") as f:
            f.write(FIXTURE.read_text())
    table = _table(client, "read_records", str(tmp_path / "*.jsonl.gz"), "events")
    assert table.num_rows == 500
    assert table.schema.field("ts").type == TIMESTAMP_TYPE
    assert sorted({int(v) for v in table.column("id").to_pylist() if v is not None}) == list(range(1, 251))


def test_read_records_unknown_stream(client: Client) -> None:
    with pytest.raises(Exception, match="no SCHEMA message"):
        _table(client, "read_records", str(FIXTURE), "nope")


def test_read_messages_from_url(client: Client) -> None:
    import functools
    import http.server
    import threading

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(FIXTURE.parent))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/{FIXTURE.name}"
        table = _table(client, "read_records", url, "users")
        assert table.column("id").to_pylist() == [1, 2, 3]
        with pytest.raises(Exception, match="HTTP 404"):
            _table(client, "read_messages", url + ".missing")
    finally:
        server.shutdown()


def test_launch_argv_runs_python_shebang_scripts_through_the_interpreter_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows cannot exec a ``#!`` script, so a Python-shebang tap runs via ``sys.executable``."""
    import sys

    native = tmp_path / "tap-native.exe"
    native.write_bytes(b"MZ")
    other = tmp_path / "tap-shell"
    other.write_text("#!/bin/sh\necho hi\n")
    if sys.platform != "win32":
        assert tapmod._launch_argv(FAKE_TAP) == [FAKE_TAP]
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("PATHEXT", ".COM;.EXE;.BAT;.CMD")
    assert tapmod._launch_argv(FAKE_TAP) == [sys.executable, FAKE_TAP]
    assert tapmod._launch_argv(str(native)) == [str(native)]
    assert tapmod._launch_argv(str(other)) == [str(other)]


def test_batch_manifest_paths() -> None:
    """file:// manifests resolve to local paths, including the bare ``file://`` + Windows-path form."""
    from singer_worker.messages import _local_path

    assert _local_path("file:///tmp/users-0001.json.gz") == Path("/tmp/users-0001.json.gz")
    assert _local_path("/tmp/x.json.gz") == Path("/tmp/x.json.gz")
    assert str(_local_path("file://C:\\Users\\me\\x.json.gz")) == str(Path("C:\\Users\\me\\x.json.gz"))
    assert str(_local_path("file://D:/a/x.json.gz")) == str(Path("D:/a/x.json.gz"))
    with pytest.raises(ValueError):
        _local_path("s3://bucket/x.json.gz")
