from __future__ import annotations

import enum
import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # pragma: no cover
    from _typeshed import StrOrBytesPath


class StatusEnum(enum.Enum):
    failed = -2  # Processing failed
    rejected = -1  # Marked as rejected
    waiting = 0  # Prerequisites not ready
    ready = 1  # Ready to run
    preparing = 2  # Inputs are being prepared
    prepared = 3  # Inputs have been prepared
    pending = 4  # Jobs are queued for submission
    running = 5  # Jobs are running
    collectable = 6  # Jobs have finshed running, can collect results
    collecting = 7  # Jobs have finshed running, collecting results
    completed = 8  # Completed, awaiting review
    validating = 9  # Running validation scripts
    reviewable = 10  # Ready to review
    accepted = 11  # Completed, reviewed and accepted

    def bad(self) -> bool:
        """Can be used to filter out failed and rejected runs"""
        return self.value < 0


class LevelEnum(enum.Enum):
    production = 0  # A family of related campaigns
    campaign = 1  # A full data processing campaign
    step = 2  # Part of a campaign that is finished before moving on
    group = 3  # A subset of data that can be processed in paralllel as part of a step

    def parent(self) -> Optional[LevelEnum]:
        """Return the parent level, or `None` if does not exist"""
        if self.value == 0:
            return None
        return LevelEnum(self.value - 1)

    def child(self) -> Optional[LevelEnum]:
        """Return the child level, or `None` if does not exist"""
        if self.value == 3:
            return None
        return LevelEnum(self.value + 1)


class TableEnum(enum.Enum):
    production = 0
    campaign = 1
    step = 2
    group = 3
    workflow = 4
    script = 5
    dependency = 6


class InputType(enum.Enum):
    source = 0  # Use the source collection
    tagged = 1  # Make a TAGGED collection
    chained = 1  # Make a CHAINED collection


class OutputType(enum.Enum):
    run = 0  # Write directly to a RUN collection
    tagged = 1  # Collect results into a TAGGED collection
    chained = 2  # Collect results into a CHAINED collection


class ScriptType(enum.Enum):
    prepare = 0  # Called before the workflow is run
    collect = 1  # Called after the workflows have been run
    validate = 2  # Called after collection


class ScriptMethod(enum.Enum):
    no_script = 0  # No actual script, just a placeholder
    bash_stamp = 1  # Bash script that writes a stamp file
    bash_callback = 2  # Bash script that calls back to cm
    bash_url = 3  # Bash script that use a URL to check status


def safe_makedirs(path: StrOrBytesPath) -> None:
    try:
        os.makedirs(path)
    except OSError:
        pass
