from __future__ import annotations

from pathlib import Path

import pandas as pd


def save_dataframe(
    df: pd.DataFrame,
    path: Path,
) -> None:
    """
    Save a dataframe to a CSV file.

    Args:
        df: The dataframe to save.
        path: The path where the CSV file will be saved.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)