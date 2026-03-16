from __future__ import annotations

from pathlib import Path

import matplotlib.cm as cm
import numpy as np
import pandas as pd

from .base import create_figure, finalize_chart, prepare_dataframe


def plot_bar(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    output_path: Path,
    rotate_x: int | None = None,
    colors: dict[str, str] | None = None,
) -> None:
    """
    Plot a standard bar chart.
    """
    df = prepare_dataframe(df, x_col, y_col).copy()

    if pd.api.types.is_numeric_dtype(df[x_col]):
        df = df.sort_values(x_col)
    else:
        df = df.sort_values(y_col, ascending=False)

    fig, ax = create_figure()

    if colors:
        bar_colors = [colors.get(str(x), "#4C78A8") for x in df[x_col]]
    else:
        bar_colors = cm.tab20(np.linspace(0, 1, len(df)))

    ax.bar(
        df[x_col],
        df[y_col],
        color=bar_colors,
    )

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
        rotate_x=rotate_x,
    )
    
def plot_stacked_bar(
    df: pd.DataFrame,
    x_col: str,
    stack_cols: list[str],
    labels: list[str],
    title: str,
    output_path: Path,
    colors: dict[str, str] | None = None,
    rotate_x: int | None = None,
) -> None:
    """
    Plot stacked bar chart.

    Parameters
    ----------
    df : pd.DataFrame
        Data containing categories and stacked values.

    x_col : str
        Column used for x-axis categories.

    stack_cols : list[str]
        Columns containing numeric values to stack.

    labels : list[str]
        Labels corresponding to each stacked column.

    title : str
        Chart title.

    output_path : Path
        Destination path for the saved chart.

    colors : dict[str, str], optional
        Mapping of label -> color.

    rotate_x : int | None
        Optional x-axis label rotation.
    """
    df = prepare_dataframe(df, x_col, *stack_cols).copy()

    if len(stack_cols) != len(labels):
        raise ValueError("stack_cols and labels must have the same length")

    # Choose sorting strategy based on x-axis type:
    # - For numeric/datetime-like x_col, preserve natural/chronological order.
    # - For categorical x_col, sort bars by total size for readability.
    is_numeric_x = pd.api.types.is_numeric_dtype(df[x_col])
    is_datetime_x = (
        pd.api.types.is_datetime64_any_dtype(df[x_col])
        or pd.api.types.is_period_dtype(df[x_col])
    )
    if is_numeric_x or is_datetime_x:
        df = df.sort_values(x_col)
    else:
        df["total"] = df[stack_cols].sum(axis=1)
        df = df.sort_values("total", ascending=False)
    fig, ax = create_figure()

    bottom = np.zeros(len(df))

    for col, label in zip(stack_cols, labels):

        color = colors.get(label) if colors else None

        ax.bar(
            df[x_col],
            df[col],
            bottom=bottom,
            label=label,
            color=color,
        )

        bottom += df[col].to_numpy()

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel="count",
        output_path=output_path,
        legend=True,
        rotate_x=rotate_x,
    )