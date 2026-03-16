"""
Defines configuration constants for styling and formatting charts in the analytics module. 
Includes default figure settings and grid styling for consistent visual presentation across all charts.
"""
from __future__ import annotations

# --------------------------------------------------
# Figure configuration
# --------------------------------------------------
"""
DPI is the resolution of the figure in dots per inch, which affects the clarity and quality of the saved charts.
FIGSIZE is the default size of the figure in inches, defined as a tuple (width, height). 
These settings ensure that all charts have a consistent appearance and are of high quality when saved or displayed."""
DEFAULT_DPI: int = 300
DEFAULT_FIGSIZE: tuple[int, int] = (12, 7)


# --------------------------------------------------
# Style configuration
# --------------------------------------------------
"""
DEFAULT_STYLE is the default style for all charts, which defines the overall look and feel of the charts (e.g., colors, grid, background).
seaborn-v0_8-whitegrid is a clean and modern style that includes a white background and gridlines, making it suitable for data visualization in the analytics module.
"""
DEFAULT_STYLE: str = "seaborn-v0_8-whitegrid"

"""
Title font size is the size of the font used for chart titles
Label font size is the size of the font used for axis labels and legend text.
Tick font size is the size of the font used for axis tick labels.
Legend font size is the size of the font used for legend text.
These settings ensure that all text elements in the charts are legible and consistent across different charts.
"""
TITLE_FONT_SIZE: int = 14
LABEL_FONT_SIZE: int = 11
TICK_FONT_SIZE: int = 10
LEGEND_FONT_SIZE: int = 10


# --------------------------------------------------
# Grid configuration
# --------------------------------------------------
"""
GRID_ENABLED is a boolean that indicates whether gridlines should be displayed on the charts.
GRID_ALPHA is the transparency level of the gridlines, where 0 is fully transparent and 1 is fully opaque.
GRID_STYLE defines the line style of the gridlines (e.g., solid, dashed, dotted).
These settings ensure that gridlines are consistently styled across all charts, enhancing readability
"""
GRID_ENABLED: bool = True
GRID_ALPHA: float = 0.4
GRID_STYLE: str = "--"

DIFFICULTY_COLORS = {
    "Advanced": "#E78AC3",         # pink
    "Intermediate": "#FFD92F",     # yellow
    "Beginner": "#8DA0CB",         # purple / lavender
    "Good First Issue": "#66C2A5", # light green
    "Unknown": "#B3B3B3",          # neutral grey
}

ONBOARDING_COLORS = {
    "Good First Issues": "#2E749F",            # navy
    "Good First Issue Candidates": "#D8A251",  # dark pink
}

STATE_COLORS = {
    "total": "#3D3D3D", # dark grey
    "closed": "#28A197",# turquoise
    "open": "#F46A25",  # orange
}