from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LabelSpec:
    """
    Represents a logical label group used for analytics.
    """

    name: str
    labels: set[str]

    def __post_init__(self):
        object.__setattr__(
            self,
            "labels",
            {l.lower() for l in self.labels},
        )

    def __or__(self, other: LabelSpec) -> LabelSpec:
        """
        Combine label groups.
        """
        return LabelSpec(
            name=f"{self.name} + {other.name}",
            labels=self.labels | other.labels,
        )
    
    def matches(self, labels: set[str]) -> bool:
        """
        Return True if any of the given labels matches this spec's labels.
        The input is a set of label names; matching is case-insensitive and
        succeeds when there is at least one common label.
        """
        normalized = {l.lower() for l in labels}
        return bool(normalized & self.labels)


GOOD_FIRST_ISSUE = LabelSpec(
    name="Good First Issues",
    labels={
        "good first issue",
        "skill: good first issue",
    },
)

GOOD_FIRST_ISSUE_CANDIDATE = LabelSpec(
    name="Good First Issue Candidates",
    labels={
        "good first issue candidate",
    },
)

ALL_ONBOARDING = GOOD_FIRST_ISSUE | GOOD_FIRST_ISSUE_CANDIDATE

BUG = LabelSpec(
    name="Bug Reports",
    labels={"bug"},
)

DIFFICULTY_GOOD_FIRST_ISSUE = LabelSpec(
    name="Good First Issue",
    labels={
        "Good First Issue",
        "skill: Good First Issue",
    },
)

DIFFICULTY_BEGINNER = LabelSpec(
    name="Beginner",
    labels={
        "beginner",
    },
)

DIFFICULTY_INTERMEDIATE = LabelSpec(
    name="Intermediate",
    labels={
        "intermediate",
    },
)

DIFFICULTY_ADVANCED = LabelSpec(
    name="Advanced",
    labels={
        "advanced",
    },
)

DIFFICULTY_LEVELS = (
    DIFFICULTY_GOOD_FIRST_ISSUE,
    DIFFICULTY_BEGINNER,
    DIFFICULTY_INTERMEDIATE,
    DIFFICULTY_ADVANCED,
)

UNKNOWN_DIFFICULTY = "Unknown"

DIFFICULTY_ORDER = [
    UNKNOWN_DIFFICULTY,
    DIFFICULTY_GOOD_FIRST_ISSUE.name,
    DIFFICULTY_BEGINNER.name,
    DIFFICULTY_INTERMEDIATE.name,
    DIFFICULTY_ADVANCED.name,
]