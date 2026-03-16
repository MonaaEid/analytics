"""
Low-level GitHub HTTP client.

Handles authentication, connection reuse, retries,
GraphQL rate-limit awareness, and request execution
for both REST and GraphQL GitHub API calls.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, TypedDict

import requests

from hiero_analytics.config.github import (
    BASE_URL,
    GITHUB_TOKEN,
    HTTP_TIMEOUT_SECONDS,
    REQUEST_DELAY_SECONDS,
)

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


# --------------------------------------------------------
# TYPES
# --------------------------------------------------------

JSON = dict[str, Any]


class GraphQLRateLimit(TypedDict, total=False):
    """
    GraphQL returns rate limit information.
        cost: cost of the query
        remaining: available api requests 
        limit: maximum api requests
        resetAt: limit resets each hour, notifies you when that is

    """
    cost: int
    remaining: int
    limit: int
    resetAt: str


# --------------------------------------------------------
# HEADERS
# --------------------------------------------------------

def github_headers() -> dict[str, str]:
    """
    Build HTTP headers used for GitHub API requests.
    This is required for github to accept the query.
    """
    headers: dict[str, str] = {
        "User-Agent": "hiero-analytics",
        "Accept": "application/vnd.github+json",
    }

    if not GITHUB_TOKEN:
        logger.warning(
            "GITHUB_TOKEN not set. Unauthenticated rate limit is 60 requests/hour."
        )
        return headers

    logger.info(
        "Using GITHUB_TOKEN for authenticated requests. API allows up to 5000 requests per hour."
    )

    # Pass the Github Token for higher rate limits
    headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers


# --------------------------------------------------------
# CLIENT
# --------------------------------------------------------

class GitHubClient:
    """HTTP client for interacting with the GitHub API."""

    def __init__(self) -> None:
        """
        Initialize the client with a persistent HTTP session and
        authentication headers.
        """
        # Reusable HTTP session with default headers for authentication and content type
        self.session: requests.Session = requests.Session()
        self.session.headers.update(github_headers())

        # usage counters to keep track of API usage 
        self.requests_made: int = 0
        # this is the graphQL counter which has uses variable costs 
        self.cost_used: int = 0

    # --------------------------------------------------------
    # USAGE REPORTING
    # --------------------------------------------------------

    def log_usage(self) -> None:
        """Log API usage statistics."""
        logger.info(
            "GitHub API usage: %d requests, %d GraphQL points used",
            self.requests_made,
            self.cost_used,
        )

    # --------------------------------------------------------
    # RATE LIMIT HANDLING
    # --------------------------------------------------------

    def _handle_graphql_rate(self, data: JSON) -> None:
        """Handle GraphQL rate-limit reporting and throttling."""
        rate: GraphQLRateLimit | None = (data.get("data") or {}).get("rateLimit")

        if not rate:
            return

        remaining = rate.get("remaining")
        cost = rate.get("cost")
        limit = rate.get("limit")
        reset_at = rate.get("resetAt")

        logger.debug(
            "GraphQL cost=%s remaining=%s/%s",
            cost,
            remaining,
            limit,
        )

        if reset_at:

            reset_time = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
            now = datetime.now(UTC)

            seconds_until_reset = int((reset_time - now).total_seconds())

            logger.debug(
                "GraphQL rate limit resets at %s (%ds)",
                reset_time.isoformat(),
                max(seconds_until_reset, 0),
            )

        if remaining is not None and remaining < 50:

            logger.warning(
                "GraphQL budget low (%s remaining). Pausing briefly.",
                remaining,
            )

            time.sleep(5)

    # --------------------------------------------------------
    # ERROR HANDLING
    # --------------------------------------------------------

    def _handle_graphql_errors(
        self,
        data: JSON,
        method: str,
        url: str,
        kwargs: Mapping[str, Any],
    ) -> JSON | None:
        """Detect GraphQL errors and retry if rate-limited."""
        errors = data.get("errors")

        if not errors:
            return None

        for err in errors:

            if err.get("type") == "RATE_LIMIT":

                logger.warning("GraphQL rate limit exceeded.")

                rate: GraphQLRateLimit | None = (data.get("data") or {}).get("rateLimit")

                if rate and rate.get("resetAt"):

                    reset_time = datetime.fromisoformat(
                        rate["resetAt"].replace("Z", "+00:00")
                    )

                    now = datetime.now(UTC)
                    wait_seconds = int((reset_time - now).total_seconds())

                    wait_seconds = max(wait_seconds, 60)

                    logger.warning(
                        "Sleeping %ds until rate limit reset at %s",
                        wait_seconds,
                        reset_time.isoformat(),
                    )

                    time.sleep(wait_seconds)

                    return self._request(method, url, **kwargs)

                logger.warning("Sleeping 300s before retrying request")

                time.sleep(300)

                return self._request(method, url, **kwargs)

        raise RuntimeError(f"GitHub GraphQL error: {data}")

    # --------------------------------------------------------
    # REQUEST EXECUTION
    # --------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> JSON:
        """
        Execute HTTP request with retries and rate-limit awareness for both
        REST (via HTTP headers) and GraphQL (via response payload).
        """
        for attempt in range(1, MAX_RETRIES + 1):

            logger.debug(
                "GitHub request → %s %s (attempt %d)",
                method,
                url,
                attempt,
            )

            start = time.time()

            try:

                response = self.session.request(
                    method,
                    url,
                    timeout=HTTP_TIMEOUT_SECONDS,
                    **kwargs,
                )

            except requests.RequestException as exc:

                if attempt == MAX_RETRIES:
                    logger.error(
                        "GitHub request failed after %d attempts",
                        MAX_RETRIES,
                    )
                    raise

                logger.warning(
                    "Request error (%s). Retrying attempt %d...",
                    exc,
                    attempt + 1,
                )

                time.sleep(2**attempt)
                continue

            elapsed = time.time() - start

            logger.debug("GitHub response ← %.2fs", elapsed)

            # --------------------------------------------------------
            # REST rate-limit handling (non-GraphQL responses)
            # --------------------------------------------------------
            is_graphql = url.endswith("/graphql")
            if not is_graphql:
                remaining_header = response.headers.get("X-RateLimit-Remaining")
                reset_header = response.headers.get("X-RateLimit-Reset")
                if remaining_header is not None and reset_header is not None:
                    try:
                        remaining = int(remaining_header)
                        reset_epoch = int(reset_header)
                    except (TypeError, ValueError):
                        remaining = None
                        reset_epoch = None
                    if remaining == 0 and reset_epoch is not None:
                        # Compute how long to wait until the rate limit resets.
                        sleep_seconds = max(0, reset_epoch - int(time.time()))
                        if response.status_code == 403 and attempt < MAX_RETRIES:
                            logger.warning(
                                "GitHub REST rate limit reached (403). "
                                "Sleeping for %ds before retrying attempt %d...",
                                sleep_seconds,
                                attempt + 1,
                            )
                            if sleep_seconds > 0:
                                time.sleep(sleep_seconds)
                            continue
                        if response.ok and sleep_seconds > 0:
                            # Successful response but no remaining budget; delay
                            # to avoid immediate rate-limit errors on subsequent calls.
                            logger.warning(
                                "GitHub REST rate limit exhausted. "
                                "Sleeping for %ds before returning response...",
                                sleep_seconds,
                            )
                            time.sleep(sleep_seconds)
            # For GraphQL and non-rate-limited REST responses, propagate HTTP errors.
            
            response.raise_for_status()

            data: JSON = response.json()

            # update counters
            self.requests_made += 1

            # --------------------------------------------------------
            # GraphQL-specific rate-limit accounting
            # --------------------------------------------------------
            rate: GraphQLRateLimit | None = (data.get("data") or {}).get("rateLimit")
            if rate:
                self.cost_used += rate.get("cost", 0)

            retry = self._handle_graphql_errors(
                data,
                method,
                url,
                kwargs,
            )

            if retry is not None:
                return retry

            self._handle_graphql_rate(data)

            if REQUEST_DELAY_SECONDS > 0:
                time.sleep(REQUEST_DELAY_SECONDS)

            return data

        raise RuntimeError("Unreachable request state")

    # --------------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------------

    def get(self, url: str, **kwargs: Any) -> JSON:
        """
        Execute a GET request to a GitHub REST endpoint.

        Args:
            url: Full GitHub API URL

        Returns:
            Parsed JSON response
        """
        return self._request("GET", url, **kwargs)

    def graphql(
        self,
        query: str,
        variables: Mapping[str, Any],
    ) -> JSON:
        """
        Execute a GraphQL query against the GitHub API.

        Args:
            query: GraphQL query string
            variables: Variables passed to the query

        Returns:
            Parsed JSON response
        """
        payload: JSON = {
            "query": query,
            "variables": dict(variables),
        }

        return self._request(
            "POST",
            f"{BASE_URL}/graphql",
            json=payload,
        )