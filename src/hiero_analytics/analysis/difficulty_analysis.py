from __future__ import annotations

import pandas as pd

from hiero_analytics.domain.labels import (
    DIFFICULTY_ADVANCED,
    DIFFICULTY_BEGINNER,
    DIFFICULTY_GOOD_FIRST_ISSUE,
    DIFFICULTY_INTERMEDIATE,
)

DIFFICULTY_GROUPS = {
    DIFFICULTY_GOOD_FIRST_ISSUE.name: DIFFICULTY_GOOD_FIRST_ISSUE.labels,
    DIFFICULTY_BEGINNER.name: DIFFICULTY_BEGINNER.labels,
    DIFFICULTY_INTERMEDIATE.name: DIFFICULTY_INTERMEDIATE.labels,
    DIFFICULTY_ADVANCED.name: DIFFICULTY_ADVANCED.labels,
}


def count_label_groups(df: pd.DataFrame, groups: dict[str, set[str]]) -> pd.DataFrame:
    """
    Count issues belonging to predefined label groups.

    For each group, the function checks whether an issue contains at least
    one label from the group. Issues matching a group are counted toward
    that group’s total.

    Parameters
    ----------
    df
        DataFrame containing an issue dataset. Must include a `labels`
        column containing label lists for each issue.
    groups
        Mapping of group names to sets of labels representing the group.

    Returns:
    -------
    pd.DataFrame
        DataFrame with columns:
        - difficulty : name of the label group
        - count      : number of issues matching that group
    """
    if df.empty:
        return pd.DataFrame(columns=["difficulty", "count"])

    rows = []

    for name, labels in groups.items():
        mask = df["labels"].map(lambda xs: bool(set(xs or []) & labels))
        rows.append({"difficulty": name, "count": int(mask.sum())})

    return pd.DataFrame(rows)


def difficulty_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the distribution of issues across defined difficulty levels.

    Difficulty levels are determined using the predefined DIFFICULTY_GROUPS
    label mapping.

    Parameters
    ----------
    df
        Issue dataframe containing a `labels` column.

    Returns:
    -------
    pd.DataFrame
        DataFrame summarizing issue counts for each difficulty level.
    """
    return count_label_groups(df, DIFFICULTY_GROUPS)


def merged_pr_difficulty_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identical to `difficulty_distribution` but named to reflect that the input dataframe is expected to be a merged dataset of pull requests.
    """
    return count_label_groups(df, DIFFICULTY_GROUPS)