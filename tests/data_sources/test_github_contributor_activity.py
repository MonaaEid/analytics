from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import hiero_analytics.data_sources.github_ingest as ingest
from hiero_analytics.data_sources.github_ingest import FetchCacheOptions
from hiero_analytics.data_sources.models import ContributorActivityRecord, RepositoryRecord


def _to_iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def test_fetch_repo_contributor_activity_graphql(monkeypatch):
    client_mock = Mock()
    monkeypatch.setattr(ingest, "paginate_cursor", lambda f: f(None)[0])
    now = datetime.now(timezone.utc)
    created_at = _to_iso(now - timedelta(days=5))
    issue_created_at = _to_iso(now - timedelta(days=6))
    issue_comment_at = _to_iso(now - timedelta(days=5, hours=18))
    pr_comment_at = _to_iso(now - timedelta(days=4, hours=12))
    reviewed_at = _to_iso(now - timedelta(days=4))
    review_comment_at = _to_iso(now - timedelta(days=4, hours=6))
    merged_at = _to_iso(now - timedelta(days=3))

    client_mock.graphql.side_effect = [
        {
            "data": {
                "repository": {
                    "pullRequests": {
                        "nodes": [
                            {
                                "number": 10,
                                "createdAt": created_at,
                                "updatedAt": merged_at,
                                "mergedAt": merged_at,
                                "author": {"login": "alice"},
                                "mergedBy": {"login": "carol"},
                                "comments": {
                                    "nodes": [
                                        {
                                            "createdAt": pr_comment_at,
                                            "author": {"login": "dave"},
                                        }
                                    ]
                                },
                                "reviews": {
                                    "nodes": [
                                        {
                                            "state": "APPROVED",
                                            "submittedAt": reviewed_at,
                                            "author": {"login": "bob"},
                                            "comments": {
                                                "nodes": [
                                                    {
                                                        "createdAt": review_comment_at,
                                                        "author": {"login": "erin"},
                                                    }
                                                ]
                                            },
                                        }
                                    ]
                                },
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        },
        {
            "data": {
                "repository": {
                    "issues": {
                        "nodes": [
                            {
                                "number": 20,
                                "createdAt": issue_created_at,
                                "author": {"login": "frank"},
                                "comments": {
                                    "nodes": [
                                        {
                                            "createdAt": issue_comment_at,
                                            "author": {"login": "grace"},
                                        }
                                    ]
                                },
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        },
    ]

    records = ingest.fetch_repo_contributor_activity_graphql(
        client_mock,
        "org",
        "repo",
        lookback_days=30,
        cache_options=FetchCacheOptions(use_cache=False),
    )

    assert len(records) == 7
    assert all(isinstance(record, ContributorActivityRecord) for record in records)
    assert {record.activity_type for record in records} == {
        "authored_pull_request",
        "commented_pull_request",
        "reviewed_pull_request",
        "commented_pull_request_review",
        "merged_pull_request",
        "created_issue",
        "commented_issue",
    }


def test_fetch_org_contributor_activity_graphql(monkeypatch):
    client_mock = Mock()
    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
    ]

    monkeypatch.setattr(ingest, "fetch_org_repos_graphql", lambda client, org, **kwargs: repos)

    monkeypatch.setattr(
        ingest,
        "fetch_repo_contributor_activity_graphql",
        lambda client, owner, repo, **kwargs: [
            ContributorActivityRecord(
                repo=f"{owner}/{repo}",
                activity_type="authored_pull_request",
                actor="alice",
                occurred_at=datetime.now(timezone.utc),
                target_type="pull_request",
                target_number=1,
            )
        ],
    )

    records = ingest.fetch_org_contributor_activity_graphql(
        client_mock,
        "org",
        max_workers=2,
        cache_options=FetchCacheOptions(use_cache=False),
    )

    assert len(records) == 2
    assert {record.repo for record in records} == {"org/repo1", "org/repo2"}
