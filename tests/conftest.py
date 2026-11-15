"""Shared fixtures: a worker client."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from vgi.client import Client

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "singer_worker.worker"]) as c:
        yield c
