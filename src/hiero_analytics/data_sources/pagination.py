"""
Generic pagination helpers for APIs.

Supports:
• Page-number pagination
• Cursor-based pagination (GraphQL style)

Adds optional logging and safety guards so long-running
pagination loops remain observable and debuggable.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 100


# --------------------------------------------------------
# PAGE NUMBER PAGINATION
# --------------------------------------------------------

def paginate_page_number(
    fetch_page: Callable[[int], list[Any]],
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int | None = None,
) -> list[Any]:
    """
    Collect items from a page-number-based API.

    Parameters
    ----------
    fetch_page:
        Callback returning items for a given page.

    page_size:
        Expected number of items per page.

    max_pages:
        Optional safety limit to stop infinite loops during debugging.
    """
    results: list[Any] = []
    page = 1

    logger.debug("Requesting pages (start_page=%d)", page)

    while True:

        logger.debug("Requesting page %d", page)

        items = fetch_page(page)

        logger.debug("Page %d returned %d items", page, len(items))

        if not items:
            logger.debug("Pagination complete (empty page)")
            break

        results.extend(items)

        if len(items) < page_size:
            logger.debug("Pagination complete (partial page)")
            break

        page += 1

        if max_pages is not None and page > max_pages:
            logger.warning("Pagination stopped after max_pages=%d", max_pages)
            break

    logger.info("Pagination collected %d items total", len(results))

    return results


# --------------------------------------------------------
# CURSOR PAGINATION (GraphQL)
# --------------------------------------------------------

def paginate_cursor(
    fetch_page: Callable[[str | None], tuple[list[Any], str | None, bool]],
    max_pages: int | None = None,
) -> list[Any]:
    """
    Generic paginator for cursor-based APIs such as GraphQL.

    fetch_page(cursor) must return:
        (items, next_cursor, has_next_page)

    Parameters
    ----------
    fetch_page:
        Function that retrieves a single page.

    max_pages:
        Optional debugging guard to stop runaway pagination.
    """
    results: list[Any] = []
    cursor: str | None = None
    page = 1

    logger.debug(
        "Requesting cursor pages (start_page=%d, start_cursor=%s)",
        page,
        cursor,
    )

    while True:

        logger.debug(
            "Requesting cursor page %d (cursor=%s)",
            page,
            cursor,
        )

        items, next_cursor, has_next = fetch_page(cursor)

        logger.debug(
            "Cursor page %d returned %d items (has_next=%s)",
            page,
            len(items),
            has_next,
        )

        results.extend(items)

        if not has_next:
            logger.debug("Cursor pagination complete")
            break

        cursor = next_cursor
        page += 1

        if max_pages is not None and page > max_pages:
            logger.warning(
                "Cursor pagination stopped after max_pages=%d",
                max_pages,
            )
            break

    logger.info("Cursor pagination collected %d items total", len(results))

    return results