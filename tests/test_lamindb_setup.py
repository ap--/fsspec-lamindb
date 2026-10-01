"""Tests that the LaminHub defaults still match those of lamindb-setup.

lamindb-setup isn't a dependency, so these tests are skipped without it. Run them with
``just test-lamindb-setup``, which installs the latest lamindb-setup for the run.
"""

from __future__ import annotations

import importlib

import pytest

from fsspec_lamindb import _hub

pytestmark = pytest.mark.lamindb_setup

pytest.importorskip("lamindb_setup")


@pytest.fixture
def environment(monkeypatch):
    # lamindb-setup uses LaminHub's production environment unless LAMIN_ENV is set
    monkeypatch.delenv("LAMIN_ENV", raising=False)
    # imported here so that a moved module fails the tests instead of skipping them
    hub_client = importlib.import_module("lamindb_setup.core._hub_client")
    return hub_client.Environment()


def test_hub_url(environment):
    assert environment.lamin_env == "prod"
    assert environment.supabase_api_url == _hub.DEFAULT_HUB_URL


def test_hub_anon_key(environment):
    assert environment.supabase_anon_key == _hub.DEFAULT_HUB_ANON_KEY
