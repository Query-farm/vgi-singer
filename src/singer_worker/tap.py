"""Install, configure and run Singer taps as subprocesses.

A Singer tap is a command line program::

    tap-foo --config config.json [--discover] [--catalog catalog.json] [--state state.json]

``--discover`` prints a catalog (JSON) on stdout; a sync prints one JSON
message per line (SCHEMA, RECORD, STATE, ACTIVATE_VERSION, BATCH).

**Installation.** A tap is named by its executable (``tap-csv``). With a
``pip_url`` (a pip requirement such as ``tap-foo==1.2`` or
``git+https://github.com/MeltanoLabs/tap-csv.git``) the tap is installed once
into its own virtual environment under the cache directory, keyed by the
spec, and reused afterwards; nothing is ever installed into the worker's own
environment. Without a ``pip_url`` the tap name itself is the requirement.
An ``executable`` path skips installation entirely.

**Config.** The tap's config JSON comes from a DuckDB secret of type
``singer``. It is written to a private (0600) temporary file for the lifetime
of the tap process and deleted as soon as the process exits.

**Streaming.** A reader thread moves stdout lines into a *bounded* queue, so a
fast tap blocks on its pipe instead of being buffered in memory. stderr is
drained into a small ring buffer that is quoted when the tap fails. A tap that
prints nothing for ``timeout_seconds`` is killed.
"""

from __future__ import annotations

import atexit
import collections
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

log = logging.getLogger(__name__)

CACHE_ENV_VAR = "VGI_SINGER_CACHE_DIR"
DEFAULT_TIMEOUT_SECONDS = 300
QUEUE_LINES = 2_000
STDERR_LINES = 40
DISCOVERY_CACHE_SECONDS = 300
IDLE_RUN_SECONDS = 600
_EOF = object()


class TapError(RuntimeError):
    """A tap could not be installed, failed, or printed something unusable."""


@dataclass(frozen=True, slots=True)
class TapSpec:
    """Which tap to run and how to obtain it."""

    tap: str
    pip_url: str = ""
    executable: str = ""
    python: str = ""

    def __post_init__(self) -> None:
        """Reject tap names that are not plain executable names."""
        if not self.executable and (not self.tap or "/" in self.tap or self.tap.startswith("-")):
            raise TapError(
                f"tap must be a tap executable name such as 'tap-csv', got {self.tap!r}; "
                "use executable := '/path/to/tap' for a tap that is already installed"
            )

    @property
    def key(self) -> str:
        """Stable identifier of the installed environment."""
        raw = json.dumps([self.tap, self.pip_url or self.tap, self.python], separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


def cache_dir() -> Path:
    """Where tap environments live: ``$VGI_SINGER_CACHE_DIR`` or the user cache directory."""
    explicit = os.environ.get(CACHE_ENV_VAR)
    if explicit:
        return Path(explicit)
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "vgi-singer"
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "vgi-singer"


def _run_install(cmd: list[str], what: str) -> None:
    log.info("singer: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900, check=False)
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip().splitlines()[-15:]
        raise TapError(f"could not {what} (exit {proc.returncode}):\n" + "\n".join(tail))


def resolve_executable(spec: TapSpec) -> str:
    """Return the tap executable, installing it into its cached environment if needed."""
    if spec.executable:
        path = Path(spec.executable).expanduser()
        if not path.is_file() or not os.access(path, os.X_OK):
            raise TapError(f"executable {spec.executable!r} is not an executable file")
        return str(path)
    env_dir = cache_dir() / "taps" / f"{spec.tap}-{spec.key}"
    exe = env_dir / "venv" / "bin" / spec.tap
    if exe.is_file():
        return str(exe)
    env_dir.mkdir(parents=True, exist_ok=True)
    with open(env_dir / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if exe.is_file():
            return str(exe)
        venv = env_dir / "venv"
        shutil.rmtree(venv, ignore_errors=True)
        requirement = spec.pip_url or spec.tap
        uv = shutil.which("uv")
        if uv:
            venv_cmd = [uv, "venv", "--quiet", str(venv)]
            if spec.python:
                venv_cmd += ["--python", spec.python]
            _run_install(venv_cmd, f"create an environment for {spec.tap}")
            _run_install(
                [uv, "pip", "install", "--quiet", "--python", str(venv / "bin" / "python"), *requirement.split()],
                f"install {requirement}",
            )
        else:
            python = shutil.which(f"python{spec.python}") if spec.python else sys.executable
            if not python:
                raise TapError(f"python {spec.python} is not installed")
            _run_install([python, "-m", "venv", str(venv)], f"create an environment for {spec.tap}")
            _run_install(
                [str(venv / "bin" / "pip"), "install", "--quiet", *requirement.split()], f"install {requirement}"
            )
        if not exe.is_file():
            shutil.rmtree(venv, ignore_errors=True)
            raise TapError(f"installing {requirement!r} did not provide an executable named {spec.tap!r}")
    return str(exe)


@contextlib.contextmanager
def private_file(content: str, suffix: str) -> Iterator[str]:
    """Yield a 0600 temporary file holding ``content``, deleted on exit."""
    fd, path = tempfile.mkstemp(prefix="vgi-singer-", suffix=suffix)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(content)
        yield path
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)


def _private_path(content: str, suffix: str) -> str:
    fd, path = tempfile.mkstemp(prefix="vgi-singer-", suffix=suffix)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


def _stderr_tail(lines: collections.deque[str]) -> str:
    return "\n".join(lines) if lines else "(no stderr output)"


# ---------------------------------------------------------------- discovery

_discovery_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_discovery_lock = threading.Lock()


def discover(spec: TapSpec, config: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Run ``tap --discover`` and return the catalog (cached briefly per tap and config)."""
    cache_key = hashlib.sha256(json.dumps([spec.key, spec.executable, config]).encode()).hexdigest()
    with _discovery_lock:
        hit = _discovery_cache.get(cache_key)
        if hit and time.monotonic() - hit[0] < DISCOVERY_CACHE_SECONDS:
            cached: dict[str, Any] = json.loads(json.dumps(hit[1]))
            return cached
    exe = resolve_executable(spec)
    with private_file(config, ".config.json") as config_path:
        try:
            proc = subprocess.run(
                [exe, "--config", config_path, "--discover"],
                capture_output=True,
                text=True,
                timeout=timeout_seconds or None,
                check=False,
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired as exc:
            raise TapError(f"{spec.tap} --discover did not finish within {timeout_seconds} seconds") from exc
    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-STDERR_LINES:])
        raise TapError(f"{spec.tap} --discover exited with code {proc.returncode}:\n{tail}")
    catalog = _parse_catalog(proc.stdout, spec.tap)
    with _discovery_lock:
        _discovery_cache[cache_key] = (time.monotonic(), catalog)
    fresh: dict[str, Any] = json.loads(json.dumps(catalog))
    return fresh


def _parse_catalog(text: str, tap: str) -> dict[str, Any]:
    try:
        catalog = json.loads(text)
    except json.JSONDecodeError:
        # Some taps print log lines on stdout before the catalog.
        start = text.find("{")
        try:
            catalog = json.loads(text[start:]) if start >= 0 else None
        except json.JSONDecodeError:
            catalog = None
    if not isinstance(catalog, dict) or not isinstance(catalog.get("streams"), list):
        raise TapError(f"{tap} --discover did not print a Singer catalog (a JSON object with 'streams')")
    return catalog


# ---------------------------------------------------------------- sync runs


@dataclass
class TapRun:
    """One running ``tap --config --catalog [--state]`` process."""

    tap: str
    proc: subprocess.Popen[str]
    files: list[str]
    lines: queue.Queue[object] = field(default_factory=lambda: queue.Queue(maxsize=QUEUE_LINES))
    stderr: collections.deque[str] = field(default_factory=lambda: collections.deque(maxlen=STDERR_LINES))
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    last_used: float = field(default_factory=time.monotonic)
    last_output: float = field(default_factory=time.monotonic)
    finished: bool = False
    # Scan position: the last STATE value seen and the items read since it.
    last_state: str = ""
    since_state: int = 0

    @classmethod
    def start(
        cls,
        spec: TapSpec,
        config: str,
        catalog: dict[str, Any] | None,
        state: str | None,
        *,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> TapRun:
        """Write the config/catalog/state files and start the tap."""
        exe = resolve_executable(spec)
        files = [_private_path(config, ".config.json")]
        cmd = [exe, "--config", files[0]]
        if catalog is not None:
            files.append(_private_path(json.dumps(catalog), ".catalog.json"))
            cmd += ["--catalog", files[-1]]
        if state:
            files.append(_private_path(state, ".state.json"))
            cmd += ["--state", files[-1]]
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError as exc:
            _unlink_all(files)
            raise TapError(f"could not start {spec.tap}: {exc}") from exc
        run = cls(tap=spec.tap, proc=proc, files=files, timeout_seconds=timeout_seconds)
        assert proc.stdout is not None and proc.stderr is not None
        threading.Thread(target=run._pump_stdout, args=(proc.stdout,), daemon=True).start()
        threading.Thread(target=run._pump_stderr, args=(proc.stderr,), daemon=True).start()
        return run

    def _pump_stdout(self, stream: IO[str]) -> None:
        try:
            for line in stream:
                self.lines.put(line)
        finally:
            self.lines.put(_EOF)

    def _pump_stderr(self, stream: IO[str]) -> None:
        for line in stream:
            self.stderr.append(line.rstrip("\n"))

    def poll_line(self, wait: float) -> str | bool | None:
        """Return the next stdout line; None at the end; False if nothing arrived within ``wait`` seconds.

        Args:
            wait: Seconds to wait for a line before returning False.

        Returns:
            The line, None once the tap exited cleanly, or False when nothing arrived yet.

        Raises:
            TapError: The tap printed nothing for ``timeout_seconds``, or exited non-zero.

        """
        if self.finished:
            return None
        try:
            item = self.lines.get(timeout=wait)
        except queue.Empty:
            if self.timeout_seconds and time.monotonic() - self.last_output > self.timeout_seconds:
                self.close()
                raise TapError(
                    f"{self.tap} printed nothing for {self.timeout_seconds} seconds and was stopped; "
                    f"raise timeout_seconds for slow sources. stderr:\n{_stderr_tail(self.stderr)}"
                ) from None
            return False
        self.last_output = self.last_used = time.monotonic()
        if item is _EOF:
            self.finished = True
            code = self.proc.wait()
            # Let the stderr thread catch up so the error quotes the last lines.
            time.sleep(0.05)
            _unlink_all(self.files)
            if code != 0:
                raise TapError(f"{self.tap} exited with code {code}. stderr:\n{_stderr_tail(self.stderr)}")
            return None
        assert isinstance(item, str)
        return item

    def next_line(self) -> str | None:
        """Return the next stdout line, or None at the end (waits up to ``timeout_seconds``)."""
        while True:
            line = self.poll_line(1.0)
            if line is not False:
                assert line is None or isinstance(line, str)
                return line

    def messages(self) -> Iterator[dict[str, Any]]:
        """Yield decoded JSON messages until the tap exits (non-JSON lines are logged and skipped)."""
        while (line := self.next_line()) is not None:
            msg = parse_message(line)
            if msg is not None:
                yield msg

    def close(self) -> None:
        """Stop the tap (if still running) and delete its files."""
        self.finished = True
        if self.proc.poll() is None:
            self.proc.kill()
            with contextlib.suppress(Exception):
                self.proc.wait(timeout=5)
        _unlink_all(self.files)


def parse_message(line: str) -> dict[str, Any] | None:
    """Decode one Singer message line; None for blank or non-message lines."""
    text = line.strip()
    if not text:
        return None
    try:
        msg = json.loads(text)
    except json.JSONDecodeError:
        log.warning("singer: skipping a non-JSON line on stdout: %.200s", text)
        return None
    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
        log.warning("singer: skipping a line that is not a Singer message: %.200s", text)
        return None
    return msg


def _unlink_all(paths: list[str]) -> None:
    for path in paths:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)


# ------------------------------------------------- live runs, keyed by scan token

_runs: dict[str, TapRun] = {}
_runs_lock = threading.Lock()


def take_run(token: str) -> TapRun | None:
    """Return the live run of a scan in this process, if any."""
    with _runs_lock:
        _reap_locked()
        return _runs.get(token)


def keep_run(token: str, run: TapRun) -> None:
    """Remember a live run so the scan's next batch continues it."""
    with _runs_lock:
        _runs[token] = run


def drop_run(token: str) -> None:
    """Forget and stop a scan's run."""
    with _runs_lock:
        run = _runs.pop(token, None)
    if run is not None:
        run.close()


def _reap_locked() -> None:
    """Stop runs nobody has read from for a while (a LIMIT query stops reading early)."""
    now = time.monotonic()
    for token, run in list(_runs.items()):
        if now - run.last_used > IDLE_RUN_SECONDS:
            run.close()
            del _runs[token]


@atexit.register
def _close_all() -> None:
    with _runs_lock:
        for run in _runs.values():
            run.close()
        _runs.clear()
