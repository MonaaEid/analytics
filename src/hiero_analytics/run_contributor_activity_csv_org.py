"""Export contributor activity summary to CSV for a GitHub organization."""

from __future__ import annotations

import logging
from collections import defaultdict

import pandas as pd

from hiero_analytics.config.paths import ORG, ensure_org_dirs
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import fetch_org_contributor_activity_graphql
from hiero_analytics.data_sources.cache import FetchCacheOptions
from hiero_analytics.data_sources.governance_config import (
    ROLE_PRIORITY,
    build_repo_role_lookup,
    fetch_governance_config,
)
from hiero_analytics.export.save import save_dataframe

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Intentionally empty: comment activity is excluded from this report.
COMMENT_ACTIVITY_TYPES: set[str] = set()

ROLE_LABELS = {
    "general_user": "General User",
    "triage": "Triage",
    "committer": "Committer",
    "maintainer": "Maintainer",
}

# Tune these weights to fit your reporting preference.
ACTIVITY_WEIGHTS = {
    "create_issues": 2,
    "review": 3,
    "merge": 2,
    "create_prs": 3,
}


def _build_activity_summary_dataframe(records, repo_role_lookup: dict[str, dict[str, str]]) -> pd.DataFrame:
    """Aggregate contributor activity events into a single summary table."""
    per_contributor: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "contributor name": "",
            "role": "general_user",
            "create issues": 0,
            "review": 0,
            "merge": 0,
            "create prs": 0,
            "weighted activity score": 0,
        }
    )

    for record in records:
        actor = (record.actor or "").strip()
        if not actor:
            continue

        actor_key = actor.lower()
        repo_name = record.repo.split("/")[-1]
        detected_role = repo_role_lookup.get(repo_name, {}).get(actor_key, "general_user")

        row = per_contributor[actor_key]
        row["contributor name"] = actor

        current_role = str(row["role"])
        if ROLE_PRIORITY[detected_role] > ROLE_PRIORITY[current_role]:
            row["role"] = detected_role

        if record.activity_type == "created_issue":
            row["create issues"] += 1
        elif record.activity_type == "reviewed_pull_request":
            row["review"] += 1
        elif record.activity_type == "merged_pull_request":
            row["merge"] += 1
        elif record.activity_type == "authored_pull_request":
            row["create prs"] += 1

    rows: list[dict[str, object]] = []
    for item in per_contributor.values():
        item["role"] = ROLE_LABELS.get(str(item["role"]), "General User")
        item["weighted activity score"] = (
            int(item["create issues"]) * ACTIVITY_WEIGHTS["create_issues"]
            + int(item["review"]) * ACTIVITY_WEIGHTS["review"]
            + int(item["merge"]) * ACTIVITY_WEIGHTS["merge"]
            + int(item["create prs"]) * ACTIVITY_WEIGHTS["create_prs"]
        )
        rows.append(item)

    if not rows:
        return pd.DataFrame(
            columns=[
                "contributor name",
                "role",
                "create issues",
                "review",
                "merge",
                "create prs",
                "weighted activity score",
            ]
        )

    df = pd.DataFrame(rows)
    return df.sort_values(
        by=["weighted activity score", "create prs", "review", "merge", "create issues"],
        ascending=False,
    ).reset_index(drop=True)


def main() -> None:
    """Fetch contributor activity records and export an aggregated CSV."""
    org_data_dir, _ = ensure_org_dirs(ORG)

    print(f"Running contributor activity export for org: {ORG}")

    gov_config = fetch_governance_config()
    repo_role_lookup = build_repo_role_lookup(gov_config)

    client = GitHubClient()
    logger.info("Fetching contributor activity for org: %s", ORG)
    
    # Force cache refresh to ensure we get fresh data with the new query structure
    cache_options = FetchCacheOptions(use_cache=False)
    records = fetch_org_contributor_activity_graphql(client, org=ORG, cache_options=cache_options, lookback_days=90)

    logger.info("Fetched %d contributor activity records", len(records))
    print(f"Fetched {len(records)} contributor activity events")

    # Log sample records for debugging
    if records:
        sample = records[0]
        logger.info("Sample record: repo=%s, actor=%s, activity_type=%s", sample.repo, sample.actor, sample.activity_type)

    summary_df = _build_activity_summary_dataframe(records, repo_role_lookup)
    output_path = org_data_dir / "contributor_activity_summary.csv"
    save_dataframe(summary_df, output_path)

    print(f"Saved {len(summary_df)} contributors to: {output_path}")


if __name__ == "__main__":
    main()
