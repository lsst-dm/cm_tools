from __future__ import annotations

import contextlib
import enum
import os
import sys
from typing import TYPE_CHECKING, Iterator, Optional

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
       make_scripts()
       If all the prequisites are `accepted` can move to `ready`

    ready = 1  # Ready to run
        -> prepare()
        This will move the entry to `preparing`

    preparing = 2  # Inputs are being prepared
        -> check_prepare_scripts()
        If all the prepare scripts have been completed can move to `prepared`

    prepared = 3  # Inputs have been prepared
        -> make_children()
        This will make any child entries

    populating = 4  # Making and preparing child entries
        -> run()
        This will mark the entry as running and allow
        for batch job submission

    running = 5  # Jobs are running
        -> check_running()
        If all the jobs / children are `completed` this can move
        to `collectable`

    collectable = 6  # Jobs have finshed running, can collect results
        -> collect()
        This will submit the command to merge the collections from
        all the children, and move to `collecting`

    collecting = 7  # Jobs have finshed running, collecting results
        -> check_collect_scripts()
        If all the collection scripts have been completed can move
        to `completed`

    completed = 8  # Completed, awaiting review
        -> validate()
        This will submit validation scripts, and move to `validating`

    validating = 9  # Running validation scripts
        -> check_validate_scripts()
        If all the validation scripts are `accepted` this will move to
        `accepted`.
        If all the validation scripts are `completed` or `accepted` this
        will move to `reviewable`

    reviewable = 10  # Ready to review
        -> accept()
        This requires outside action to move to `accept`

    accepted = 11  # Completed, reviewed and accepted
        Processing is done, can be used down the road
    """

    failed = -2
    rejected = -1
    waiting = 0
    ready = 1
    preparing = 2
    prepared = 3
    populating = 4
    running = 5
    collectable = 6
    collecting = 7
    completed = 8
    validating = 9
    reviewable = 10
    accepted = 11

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
    fragment = 8
    config = 9


class InputType(enum.Enum):
    """Defines how an entry deals with input data

    The data source location `coll_source` must provided, either
    from the configuration, or as an input from the parent entry

    This will control how `coll_input` is set.

    The options are:

    source = 0
       Use the source collection as the input collection.

    tagged = 1
        Make a TAGGED collection by copying from the source
        collection.  In this case `data_query` can be used
        to reduce the input data.

    chained = 1
        Make a CHAINED collection connecting `coll_in`
        to `coll_source`
    """

    source = 0  # Use the source collection
    tagged = 1  # Make a TAGGED collection
    chained = 2  # Make a CHAINED collection


class OutputType(enum.Enum):
    """Defines how an entry deals with output data

    This defines how to collect data from the children of
    the entry in question.

    The child entries must have `coll_out` defined.

    This will control how `coll_output` is set

    The options are:

    run = 0
        The children ran direction into `coll_out`.  No need to do
        anything.

    tagged = 1
        Make a TAGGED collection by copying from the children.
        collection.  In this case `data_query` can be used
        to reduce the input data.

    chained = 2
        Make a CHAINED collection collect the data from the
        children
    """

    run = 0  # Write directly to a RUN collection
    tagged = 1  # Collect results into a TAGGED collection
    chained = 2  # Collect results into a CHAINED collection


class ScriptType(enum.Enum):
    """Defines when scripts get called.

    This depends on what the scripts do.
    The options are:

    prepare = 0
        Called before the workflow is run to prepare the input collection

    collect = 1
        Called after the workflows have been run to collect
        collections into a single output collection

    validate = 2
        Called after collection on the output collection to validate it
    """

    prepare = 0
    collect = 1
    validate = 2


class ScriptMethod(enum.Enum):
    """Defines how to run a script

    no_script = 0
        No actual script, just a placeholder for using python
        to manipulate the input collection

    bash = 1
        Bash script, just run the script using a system call

    More methods to come...
    """

    no_script = 0  # No actual script, just a placeholder
    bash = 1  # Bash script


def safe_makedirs(path: StrOrBytesPath) -> None:
    """Utility function to make directory and catch exception
    if it already exists
    """
    try:
        os.makedirs(path)
    except OSError:
        pass


@contextlib.contextmanager
def add_sys_path(path: os.PathLike | str | None) -> Iterator[None]:
    """Temporarily add the given path to `sys.path`."""
    if path is None:
        yield
    else:
        path = os.fspath(path)
        try:
            sys.path.insert(0, path)
            yield
        finally:
            sys.path.remove(path)
