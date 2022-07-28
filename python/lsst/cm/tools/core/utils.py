from __future__ import annotations

import enum
import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # pragma: no cover
    from _typeshed import StrOrBytesPath


class StatusEnum(enum.Enum):
    """Keeps track of the status of entries

    Typically entries should move from `waiting` to `accepted`
    one step at a time.

    Bad States:
    failed = -2  # Processing failed
    rejected = -1  # Marked as rejected

    Processing states and the transitions between them:

    waiting = 0  # Prerequisites not ready
       -> check_prequisites()
       If all the prequisites are `accepted` can move to `ready`

    ready = 1  # Ready to run
        -> prepare()
        This will move the entry to `preparing`

    preparing = 2  # Inputs are being prepared
        -> check_prepare_scripts()
        If all the prepare scripts have been completed can move to `prepared`

    prepared = 3  # Inputs have been prepared
        -> run()
        This will mark the entry as running and allow
        for batch job submission

    running = 4  # Jobs are running
        -> check_running()
        If all the jobs / children are `completed` this can move
        to `collectable`

    collectable = 5  # Jobs have finshed running, can collect results
        -> collect()
        This will submit the command to merge the collections from
        all the children, and move to `collecting`

    collecting = 6  # Jobs have finshed running, collecting results
        -> check_collect_scripts()
        If all the collection scripts have been completed can move
        to `completed`

    completed = 7  # Completed, awaiting review
        -> validate()
        This will submit validation scripts, and move to `validating`

    validating = 8  # Running validation scripts
        -> check_validate_scripts()
        If all the validation scripts are `accepted` this will move to
        `accepted`.
        If all the validation scripts are `completed` or `accepted` this
        will move to `reviewable`

    reviewable = 9  # Ready to review
        -> accept()
        This requires outside action to move to `accept`

    accepted = 10  # Completed, reviewed and accepted
        Processing is done, can be used down the road
    """

    failed = -2
    rejected = -1
    waiting = 0
    ready = 1
    preparing = 2
    prepared = 3
    running = 4
    collectable = 5
    collecting = 6
    completed = 7
    validating = 8
    reviewable = 9
    accepted = 10

    def bad(self) -> bool:
        """Can be used to filter out failed and rejected runs"""
        return self.value < 0


class LevelEnum(enum.Enum):
    """Keep track of processing hierarchy

    The levels are:

    production = 0
        A family of related campaigns

    campaign = 1
        A full data processing campaign

    step = 2
        Part of a campaign that is finished before moving on

    group = 3
        A subset of data that can be processed in paralllel as part of a step

    workflow = 4
        A single workflow (which might include thousands of tasks)
    """

    production = 0
    campaign = 1
    step = 2
    group = 3
    workflow = 4

    def parent(self) -> Optional[LevelEnum]:
        """Return the parent level, or `None` if does not exist"""
        if self.value == 0:
            return None
        return LevelEnum(self.value - 1)

    def child(self) -> Optional[LevelEnum]:
        """Return the child level, or `None` if does not exist"""
        if self.value == 4:
            return None
        return LevelEnum(self.value + 1)


class TableEnum(enum.Enum):
    """Keeps track of database tables

    Largely for debugging, can be used for generic functions
    such are counting or printing tables
    """

    production = 0
    campaign = 1
    step = 2
    group = 3
    workflow = 4
    script = 5
    job = 6
    dependency = 7


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
    bash = 1  # Bash script


def safe_makedirs(path: StrOrBytesPath) -> None:
    """Utility function to make directory and catch exception
    if it already exists"""
    try:
        os.makedirs(path)
    except OSError:
        pass
