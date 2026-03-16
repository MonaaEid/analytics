
import hiero_analytics.data_sources.pagination as pagination

# ---------------------------------------------------------
# page-number pagination
# ---------------------------------------------------------

def test_paginate_page_number_multiple_pages():

    pages = {
        1: [1, 2, 3],
        2: [4, 5, 6],
        3: [],
    }

    def fetch(page):
        return pages[page]

    results = pagination.paginate_page_number(fetch, page_size=3)

    assert results == [1, 2, 3, 4, 5, 6]


def test_paginate_page_number_partial_page_stops():

    pages = {
        1: [1, 2, 3],
        2: [4],  # partial page
    }

    def fetch(page):
        return pages.get(page, [])

    results = pagination.paginate_page_number(fetch, page_size=3)

    assert results == [1, 2, 3, 4]


def test_paginate_page_number_empty_first_page():

    def fetch(page):
        return []

    results = pagination.paginate_page_number(fetch)

    assert results == []


def test_paginate_page_number_max_pages_guard():

    def fetch(page):
        return [page] * 100

    results = pagination.paginate_page_number(
        fetch,
        page_size=100,
        max_pages=2,
    )

    assert len(results) == 200


# ---------------------------------------------------------
# cursor pagination
# ---------------------------------------------------------

def test_paginate_cursor_multiple_pages():

    data = {
        None: ([1, 2], "A", True),
        "A": ([3, 4], "B", True),
        "B": ([5], None, False),
    }

    def fetch(cursor):
        return data[cursor]

    results = pagination.paginate_cursor(fetch)

    assert results == [1, 2, 3, 4, 5]


def test_paginate_cursor_single_page():

    def fetch(cursor):
        return ([1, 2], None, False)

    results = pagination.paginate_cursor(fetch)

    assert results == [1, 2]


def test_paginate_cursor_max_pages_guard():

    def fetch(cursor):
        return ([1], "next", True)

    results = pagination.paginate_cursor(
        fetch,
        max_pages=2,
    )

    assert len(results) == 2


def test_paginate_cursor_handles_empty_items():

    calls = {
        None: ([], None, False)
    }

    def fetch(cursor):
        return calls[cursor]

    results = pagination.paginate_cursor(fetch)

    assert results == []