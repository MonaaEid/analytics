import pandas as pd

from hiero_analytics.data_sources.models import ScorecardRecord
from hiero_analytics.analysis.scorecard_analysis import (
    scorecard_to_dataframe,
    scorecard_stacked_dataframe,
    CHECK_COLUMNS
)


def test_scorecard_to_dataframe_empty():
    """Test empty scorecard list return empty dataframe."""
    df = scorecard_to_dataframe([])

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["repo", "score", "date"]
    assert df.empty


def test_scorecard_to_dataframe():
    """Test scorecard can be converted to dataframe."""
    records = [
        ScorecardRecord(
            repo="repo1",
            score=7.5,
            checks={},
            date=pd.Timestamp("2026-04-01"),
        )
    ]

    df = scorecard_to_dataframe(records)

    assert len(df) == 1
    assert df.iloc[0]["repo"] == "repo1"
    assert df.iloc[0]["score"] == 7.5


def test_scorecard_stacked_dataframe_empty():
    """Test stacked plot dataframe for empty scorecard list."""
    df = scorecard_stacked_dataframe([])

    assert isinstance(df, pd.DataFrame)
    assert "repo" in df.columns
    assert "score" in df.columns
    assert "date" in df.columns

    for col in CHECK_COLUMNS:
        assert col in df.columns

    assert df.empty


def test_scorecard_stacked_dataframe_with_checks():
    """Test scorecard can be converted to dataframe used for stacked bar plot."""
    records = [
        ScorecardRecord(
            repo="repo1",
            score=8.0,
            checks={
                "Maintained": 10,
                "Code-Review": 8,
            },
            date=pd.Timestamp("2026-04-01"),
        )
    ]

    df = scorecard_stacked_dataframe(records)

    row = df.iloc[0]

  
    assert row["repo"] == "repo1"
    assert row["score"] == 8.0

    assert row["Maintained"] == 10
    assert row["Code-Review"] == 8

    for col in CHECK_COLUMNS:
        if col not in ["Maintained", "Code-Review"]:
            assert row[col] == 0

