"""
This module provides functions to search for issues on GitHub using the REST API. 
It supports pagination to handle large result sets and allows for complex search queries using GitHub's search syntax.
"""

from __future__ import annotations

from typing import Any

from .github_client import GitHubClient
from .pagination import paginate_page_number


def search_issues(
    client: GitHubClient,
    query: str,
) -> list[dict[str, Any]]:
    """
    Search GitHub issues and pull requests using the REST search API.

    Args:
        client: Authenticated GitHub client.
        query: GitHub search query string.

    Returns:
        A list of issue objects returned by the GitHub API.
    """

    def page(page_number: int) -> list[dict[str, Any]]:

        params = {
            "q": query,
            "per_page": 100,
            "page": page_number,
        }

        data = client.get(
            "https://api.github.com/search/issues",
            params=params,
        )

        items = data.get("items", [])

        return [item for item in items if isinstance(item, dict)]

    return paginate_page_number(page)