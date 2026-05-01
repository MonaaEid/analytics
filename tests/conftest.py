"""Shared pytest configuration for the analytics test suite."""

import pytest

import hiero_analytics.data_sources.cache as cache
import hiero_analytics.data_sources.github_ingest as ingest


@pytest.fixture(autouse=True)
def isolate_github_cache(monkeypatch, tmp_path):
    """Keep tests isolated from any real on-disk GitHub cache state."""
    monkeypatch.setattr(ingest, "cache", cache.GitHubRecordCache(tmp_path / "github"))
