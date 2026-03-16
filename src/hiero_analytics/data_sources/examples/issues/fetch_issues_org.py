"""
Example script: fetch all issues across an organization
"""

from hiero_analytics.config.logging import setup_logging
from hiero_analytics.config.paths import ORG
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import fetch_org_issues_graphql

setup_logging()

ORGANIZATION = ORG

def main() -> None:

    client = GitHubClient()

    issues = fetch_org_issues_graphql(
        client,
        org=ORGANIZATION,
        states=["OPEN", "CLOSED"],
        max_workers=5,
    )

    print(f"Collected {len(issues)} issues across {ORGANIZATION}")


if __name__ == "__main__":
    main()