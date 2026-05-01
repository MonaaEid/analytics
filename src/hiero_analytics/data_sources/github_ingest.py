"""
GitHub data ingestion utilities using the GraphQL API.

This module provides functions for retrieving repositories, issues, and
merged pull request metadata from GitHub. Data is fetched using cursor-
based pagination and can be aggregated across an organization with
parallel requests to improve ingestion speed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from .cache import FetchCacheOptions, GitHubRecordCache
from hiero_analytics.config.paths import load_query

from .github_client import GitHubClient

from .models import (
    BaseRecord,
    ContributorActivityRecord,
    ContributorMergedPRCountRecord,
    IssueRecord,
    PullRequestDifficultyRecord,
    RepositoryRecord,
)
from .pagination import extract_graphql_cursor_page, paginate_cursor

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseRecord)

# Initialize the cache instance
cache = GitHubRecordCache()

# --------------------------------------------------------
# GENERIC RESOURCE FETCHER ENGINE
# --------------------------------------------------------


def fetch_github_resource(
    client: GitHubClient,
    query: str,
    variables: dict,
    model_class: type[T],
    nodes_path: list[str],
    *,
    cache_key: str,
    cache_scope: str,
    cache_parameters: dict[str, object],
    context_builder: Callable[[dict], dict] | None = None,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[T]:
    """Generic engine for fetching paginated GitHub resources."""
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        cache_key,
        cache_scope,
        cache_parameters,
        model_class,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    def page(cursor: str | None) -> tuple[list[T], str | None, bool]:
        paginated_vars = dict(variables)
        paginated_vars["cursor"] = cursor

        data = client.graphql(query, paginated_vars)
        nodes, next_cursor, has_next = extract_graphql_cursor_page(data, nodes_path)

        items = []
        for node in nodes:
            context = context_builder(node) if context_builder else {}
            result = model_class.from_github_node(node, context)
            items.extend(result)

        return items, next_cursor, has_next

    records = paginate_cursor(page)
    cache.save_records(
        cache_key,
        cache_scope,
        cache_parameters,
        model_class,  # type: ignore[arg-type]
        records,  # type: ignore[arg-type]
        use_cache=opts.use_cache,
    )
    return records


def fetch_org_resource_parallel(
    client: GitHubClient,
    org: str,
    fetch_repo_func: Callable,
    model_class: type[T],
    max_workers: int,
    cache_key: str,
    cache_parameters: dict[str, object],
    repos: list[str] | None = None,
    *,
    cache_options: FetchCacheOptions | None = None,
    task_desc: str = "records",
) -> list[T]:
    """Generic engine for orchestrating parallel organization repository fetches."""
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        cache_key,
        org,
        cache_parameters,
        model_class,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    logger.info("Fetching %s across %s (max_workers=%d)", task_desc, org, max_workers)

    all_repos = fetch_org_repos_graphql(client, org, cache_options=opts)

    if repos:
        allowed = set(repos)
        all_repos = [r for r in all_repos if r.full_name in allowed or r.name in allowed]

    all_records = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_repo_func, repo): repo for repo in all_repos}
        for future in as_completed(futures):
            repo = futures[future]
            try:
                result = future.result()
                if isinstance(result, list):
                    all_records.extend(result)
                else:
                    all_records.append(result)
            except Exception as exc:
                logger.exception("Failed fetching %s for %s: %s", task_desc, repo.full_name, exc)

    logger.info("Collected %d %s across %s", len(all_records), task_desc, org)
    cache.save_records(
        cache_key,
        org,
        cache_parameters,
        model_class,
        all_records,
        use_cache=opts.use_cache,
    )
    return all_records


# --------------------------------------------------------
# FETCH REPOSITORIES
# --------------------------------------------------------

def fetch_org_repos_graphql(
    client: GitHubClient,
    org: str,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[RepositoryRecord]:
    """
    Fetch all repository full names for an organization using GraphQL.
    """
    REPOS_QUERY = load_query("repos")
    return fetch_github_resource(
        client, REPOS_QUERY, {"org": org}, RepositoryRecord, ["organization", "repositories"],
        cache_key="org_repos", cache_scope=org, cache_parameters={"org": org},
        context_builder=lambda node: {"owner": org},
        cache_options=cache_options,
    )

# --------------------------------------------------------
# FETCH ISSUES
# --------------------------------------------------------

def fetch_repo_issues_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    states: list[str] | None = None,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[IssueRecord]:
    """Fetch all issues for a repository using GraphQL."""
    ISSUES_QUERY = load_query("issues")
    norm_states = [s.upper() for s in states] if states else None
    return fetch_github_resource(
        client, ISSUES_QUERY, {"owner": owner, "repo": repo, "states": norm_states}, IssueRecord, ["repository", "issues"],
        cache_key="repo_issues", cache_scope=f"{owner}_{repo}",
        cache_parameters={"owner": owner, "repo": repo, "states": sorted(norm_states or [])},
        context_builder=lambda node: {"owner": owner, "repo": repo},
        cache_options=cache_options,
    )

def fetch_org_issues_graphql(
    client: GitHubClient,
    org: str,
    states: list[str] | None = None,
    max_workers: int = 5,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[IssueRecord]:
    """Fetch all issues across all repositories in an organization."""
    def fetch_func(repo):
        return fetch_repo_issues_graphql(
            client,
            repo.owner,
            repo.name,
            states=states,
            cache_options=cache_options,
        )
    return fetch_org_resource_parallel(
        client, org, fetch_func, IssueRecord, max_workers, "org_issues",
        {"org": org, "states": sorted(s.upper() for s in states) if states else []},
        task_desc="organization issues",
        cache_options=cache_options,
    )

# --------------------------------------------------------
# FETCH MERGED PR DIFFICULTY
# --------------------------------------------------------
def fetch_repo_merged_pr_difficulty_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[PullRequestDifficultyRecord]:
    """
    Fetch merged pull requests and their linked closing issues for a repository.
    """
    MERGED_PR_QUERY = load_query("merged_pr")
    return fetch_github_resource(
        client, MERGED_PR_QUERY, {"owner": owner, "repo": repo}, PullRequestDifficultyRecord, ["repository", "pullRequests"],
        cache_key="repo_merged_pr_difficulty", cache_scope=f"{owner}_{repo}", 
        cache_parameters={"owner": owner, "repo": repo},
        context_builder=lambda node: {"owner": owner, "repo": repo},
        cache_options=cache_options,
    )

def fetch_org_merged_pr_difficulty_graphql(
    client: GitHubClient,
    org: str,
    max_workers: int = 5,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[PullRequestDifficultyRecord]:
    """
    Fetch merged pull request difficulty records across all repositories in an organization.
    """
    def fetch_func(repo):
        return fetch_repo_merged_pr_difficulty_graphql(
            client,
            repo.owner,
            repo.name,
            cache_options=cache_options,
        )
    return fetch_org_resource_parallel(
        client, org, fetch_func, PullRequestDifficultyRecord, max_workers, "org_merged_pr_difficulty",
        {"org": org}, task_desc="merged PR difficulty records",
        cache_options=cache_options,
    )

# --------------------------------------------------------
# FETCH CONTRIBUTOR ACTIVITY
# --------------------------------------------------------

def fetch_repo_contributor_activity_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    lookback_days: int = 183,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[ContributorActivityRecord]:
    """
    Fetch contributor activity signals from pull request and issue lifecycle data.

    Signals include:
    - authored_pull_request
    - reviewed_pull_request
    - merged_pull_request
    - created_issue
    """
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        "repo_contributor_activity",
        f"{owner}_{repo}",
        {"owner": owner, "repo": repo, "lookback_days": lookback_days},
        ContributorActivityRecord,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    pr_records = fetch_github_resource(
        client,
        load_query("contributor_activity"),
        {"owner": owner, "repo": repo},
        ContributorActivityRecord,
        ["repository", "pullRequests"],
        cache_key="repo_contributor_activity_prs",
        cache_scope=f"{owner}_{repo}",
        cache_parameters={"owner": owner, "repo": repo, "lookback_days": lookback_days},
        context_builder=lambda node: {
            "owner": owner,
            "repo": repo,
            "cutoff": cutoff,
            "activity_source": "pull_request",
        },
        cache_options=FetchCacheOptions(use_cache=False),
    )

    issue_records = fetch_github_resource(
        client,
        load_query("contributor_issue_activity"),
        {"owner": owner, "repo": repo},
        ContributorActivityRecord,
        ["repository", "issues"],
        cache_key="repo_contributor_activity_issues",
        cache_scope=f"{owner}_{repo}",
        cache_parameters={"owner": owner, "repo": repo, "lookback_days": lookback_days},
        context_builder=lambda node: {
            "owner": owner,
            "repo": repo,
            "cutoff": cutoff,
            "activity_source": "issue",
        },
        cache_options=FetchCacheOptions(use_cache=False),
    )

    records = [*pr_records, *issue_records]
    return records

def fetch_org_contributor_activity_graphql(
    client: GitHubClient,
    org: str,
    max_workers: int = 5,
    *,
    repos: list[str] | None = None,
    lookback_days: int = 183,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[ContributorActivityRecord]:
    """
    Fetch contributor activity records across all repositories in an organization.
    """
    def fetch_func(repo):
        return fetch_repo_contributor_activity_graphql(
            client,
            repo.owner,
            repo.name,
            lookback_days=lookback_days,
            cache_options=cache_options,
        )
    return fetch_org_resource_parallel(
        client, org, fetch_func, ContributorActivityRecord, max_workers, "org_contributor_activity",
        {"org": org, "repos": sorted(repos) if repos else [], "lookback_days": lookback_days}, repos=repos,
        task_desc="contributor activity",
        cache_options=cache_options,
    )

# --------------------------------------------------------
# FETCH CONTRIBUTOR MERGED PR COUNT
# --------------------------------------------------------

def fetch_repo_contributor_merged_pr_count_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    login: str,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> ContributorMergedPRCountRecord:
    """
    Fetch contributor merged pull request count for a specific user in a repository.
    """
    CONTRIBUTOR_MERGED_PRS_COUNT_QUERY = load_query("contributor_merged_prs_count")
    records = fetch_github_resource(
        client, CONTRIBUTOR_MERGED_PRS_COUNT_QUERY, {"searchQuery": f"is:pr is:merged author:{login} repo:{owner}/{repo}"},
        ContributorMergedPRCountRecord, ["search"],
        cache_key="repo_contributor_merged_pr_count", cache_scope=f"{owner}_{repo}_{login}",
        cache_parameters={"owner": owner, "repo": repo, "login": login},
        context_builder=lambda node: {"owner": owner, "repo": repo, "login": login},
        cache_options=cache_options,
    )
    return records[0] if records else ContributorMergedPRCountRecord(repo=f"{owner}/{repo}", login=login, merged_pr_count=0)

def fetch_org_contributor_merged_pr_count_graphql(
    client: GitHubClient,
    org: str,
    login: str,
    repos: list[str] | None = None,
    max_workers: int = 5,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[ContributorMergedPRCountRecord]:
    """Fetch contributor merged pull request count for a specific user in an org"""
    def fetch_func(repo):
        return fetch_repo_contributor_merged_pr_count_graphql(
            client,
            repo.owner,
            repo.name,
            login=login,
            cache_options=cache_options,
        )
    return fetch_org_resource_parallel(
        client, org, fetch_func, ContributorMergedPRCountRecord,
        max_workers, "org_contributor_merged_pr_count",
        {"org": org, "login": login, "repos": sorted(repos) if repos else []}, repos=repos,
        task_desc=f"merged PR count for {login}",
        cache_options=cache_options,
    )
