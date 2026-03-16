from unittest.mock import Mock

import pytest

import hiero_analytics.data_sources.github_search as search

# ---------------------------------------------------------
# fixtures
# ---------------------------------------------------------

@pytest.fixture
def mock_client():
    return Mock()


@pytest.fixture
def bypass_pagination(monkeypatch):
    """
    Replace paginate_page_number so only one page executes.
    """
    monkeypatch.setattr(
        search,
        "paginate_page_number",
        lambda f: f(1),
    )


# ---------------------------------------------------------
# basic search
# ---------------------------------------------------------

def test_search_issues_returns_items(mock_client, bypass_pagination):

    mock_client.get.return_value = {
        "items": [
            {"id": 1, "title": "issue1"},
            {"id": 2, "title": "issue2"},
        ]
    }

    results = search.search_issues(mock_client, "label:bug")

    assert len(results) == 2
    assert results[0]["id"] == 1


# ---------------------------------------------------------
# request parameters
# ---------------------------------------------------------

def test_search_issues_calls_request_correctly(mock_client, bypass_pagination):

    mock_client.get.return_value = {"items": []}

    search.search_issues(mock_client, "repo:org/repo is:issue")

    args, kwargs = mock_client.get.call_args

    assert args[0] == "https://api.github.com/search/issues"

    params = kwargs["params"]

    assert params["q"] == "repo:org/repo is:issue"
    assert params["per_page"] == 100
    assert params["page"] == 1


# ---------------------------------------------------------
# filters non-dict items
# ---------------------------------------------------------

def test_search_issues_filters_invalid_items(mock_client, bypass_pagination):

    mock_client.get.return_value = {
        "items": [
            {"id": 1},
            "bad",
            None,
            {"id": 2},
        ]
    }

    results = search.search_issues(mock_client, "test")

    assert len(results) == 2
    assert all(isinstance(i, dict) for i in results)


# ---------------------------------------------------------
# empty response
# ---------------------------------------------------------

def test_search_issues_handles_missing_items(mock_client, bypass_pagination):

    mock_client.get.return_value = {}

    results = search.search_issues(mock_client, "test")

    assert results == []


# ---------------------------------------------------------
# pagination integration
# ---------------------------------------------------------

def test_search_issues_uses_pagination(monkeypatch, mock_client):

    called = {"value": False}

    def fake_paginator(page_func):
        called["value"] = True
        return page_func(1)

    monkeypatch.setattr(search, "paginate_page_number", fake_paginator)

    mock_client.get.return_value = {"items": []}

    search.search_issues(mock_client, "test")

    assert called["value"] is True