from __future__ import annotations

from typing import Any, Optional, TextIO

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum


class TableBase:
    @classmethod
    def insert_values(cls, dbi: DbInterface, **kwargs: Any) -> Any:
        raise NotImplementedError()

    @classmethod
    def get(cls, dbi: DbInterface, row_id: int) -> Any:
        raise NotImplementedError()

    @classmethod
    def update_values(cls, dbi: DbInterface, row_id: int, **kwargs: Any) -> Any:
        raise NotImplementedError()


class ScriptBase(TableBase):
    """Interface class for database entries describing Scripts

    This will require the derived class to implement
    a `check_status` method to check on the status of the script.
    """

    script_url = ""
    log_url = ""
    id = -1

    @classmethod
    def check_status(cls, dbi: DbInterface, entry: Any) -> StatusEnum:
        raise NotImplementedError()

    @classmethod
    def rollback_script(cls, dbi: DbInterface, entry: Any) -> None:
        raise NotImplementedError()


class WorkflowBase(TableBase):
    """Interface class for database entries describing Workflows"""

    @classmethod
    def check_status(cls, dbi: DbInterface, entry: Any) -> StatusEnum:
        raise NotImplementedError()

    @classmethod
    def rollback_script(cls, dbi: DbInterface, entry: Any) -> None:
        raise NotImplementedError()


class DependencyBase:
    """Interface class for database entries describing Dependencies"""

    @classmethod
    def add_prerequisite(cls, dbi: DbInterface, depend_id: DbId, prereq_id: DbId) -> DependencyBase:
        raise NotImplementedError()


class CMTableBase(TableBase):
    """Interface class for database entries describing parts of
    the data processing

    This will require the derived class to implement
    a `get_handler` method to get a callback handler,
    and a `get_insert_fields` method to populate
    the fields need to make a new entry in the associated table.
    """

    def get_handler(self) -> Handler:
        """Return the associated callback `Handler`"""
        raise NotImplementedError()


class DbInterface:
    """Base class for database interface

    Many of the interface functions here take a DbId argument.
    This can specify either a single database entry, or all of the
    entries in a given table with a particular parent.

    In short, depending on the function, the user either must provide
    either a DbId specifying a single entry, or they use may provide
    DbId specifying multiple entries from a particular table.

    Internally, the DbInterface will use DbId to select entries for the
    requested operation.
    """

    def connection(self) -> Any:
        """Return the database connection object"""
        raise NotImplementedError()

    def get_db_id(self, level: LevelEnum, **kwargs: Any) -> DbId:
        """Return an id that identifies one or more database entries

        Parameters
        ----------
        level : LevelEnum
            Specifies which database table to create ID for

        Keywords
        --------
        These are used to specify particular values for the entry in question
        and any parent entries

        Returns
        -------
        db_id : DbId
            The requested database ID
        """
        raise NotImplementedError()

    def get_entry(self, level: LevelEnum, db_id: DbId) -> CMTableBase:
        """Return a selected entry

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Database ID specifying which row to select.
            See class notes above.

        Returns
        -------
        entry : CMTableBase
            Selected entry
        """
        raise NotImplementedError()

    def get_script(self, script_id: int) -> ScriptBase:
        """Return the info about a selected script

        Parameters
        ----------
        script_id : int
            The id for the selected script

        Returns
        -------
        script_data :
        """
        raise NotImplementedError()

    def get_workflow(self, workflow_id: int) -> WorkflowBase:
        """Return the info about a selected Workflow

        Parameters
        ----------
        workflow_id : int
            The id for the selected workflow

        Returns
        -------
        workflow_data : WorkflowBase
        """
        raise NotImplementedError()

    def print_(self, stream: TextIO, which_table: TableEnum, db_id: DbId) -> None:
        """Print a database entry or entries

        Parameters
        ----------
        stream : TextIO
            The stream we will print to

        which_table: TableEnum
            Selects which database table to print

        db_id: DbId
            Database ID specifying which entries to print.
            See class notes above.
        """
        raise NotImplementedError()

    def print_table(self, stream: TextIO, which_table: TableEnum) -> None:
        """Print a database table

        Parameters
        ----------
        stream : TextIO
            The stream we will print to

        which_table: TableEnum
            Selects which database table to print
        """
        raise NotImplementedError()

    def count(self, which_table: TableEnum, db_id: Optional[DbId]) -> int:
        """Count the number of database entries matching conditions

        Parameters
        ----------
        whichTable: TableEnum
            Selects which database table to search

        db_id: DbId
            Database ID specifying which entries to count.
            See class notes above.

        Returns
        -------
        count : int
            The number of selected rows
        """
        raise NotImplementedError()

    def update(self, level: LevelEnum, row_id: int, **kwargs: Any) -> None:
        """Update a particular database entry

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        row_id: int
            Database ID specifying which entry to update.
            See class notes above.

        Keywords
        --------
        These are passed to the handler which can use
        them to derive the values for the fields to update.
        """
        raise NotImplementedError()

    def check(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Check all database entries at a particular level

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Selects which entries to check

        """
        raise NotImplementedError()

    def add_prerequisite(self, depend_id: DbId, prereq_id: DbId) -> DependencyBase:
        """Add a prerequisite to

        Parameters
        ----------
        depend_id : DbId
            The dependent entry

        prereq_id : DbId
            The prerequiste entry
        """
        raise NotImplementedError()

    def add_script(self, **kwargs: Any) -> ScriptBase:
        """Insert a new row with details about a script

        Keywords
        --------
        Keywords are based to the

        Returns
        -------
        script : ScriptBase
            The info for the new script
        """
        raise NotImplementedError()

    def add_workflow(self, **kwargs: Any) -> WorkflowBase:
        """Insert a new row with details about a workflow

        Keywords
        --------
        Keywords are based to the

        Returns
        -------
        workflow : WorkflowBase
            The info for the new Workflow
        """
        raise NotImplementedError()

    def insert(self, parent_db_id: DbId, handler: Handler, **kwargs: Any) -> CMTableBase:
        """Insert a new database entry at a particular level

        Parameters
        ----------
        parent_db_id : DbId
            Specifies the parent entry to the entry we are inserting

        Keywords
        --------
        These are used to select the parent rows to the
        entry being inserted.
        See class notes above.

        Returns
        -------
        new_entry : CMTableBase
            The new entry
        """
        raise NotImplementedError()

    def prepare(self, level: LevelEnum, db_id: DbId, **kwargs: Any) -> list[DbId]:
        """Prepare a database entry for execution

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Database ID for this entry

        Returns
        -------
        entries : list[DbId]
            The entries that were prepared

        Keywords
        --------
        Keywords can be based to the callback handler
        """
        raise NotImplementedError()

    def queue_workflows(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Queue all the ready workflows matching the selection

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are queuing

        Returns
        -------
        entries : list[DbId]
            The entries that were queued
        """
        raise NotImplementedError()

    def launch_workflows(self, level: LevelEnum, db_id: DbId, max_running: int) -> list[DbId]:
        """Launch all the pending workflows matching the selection

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are queuing

        max_running: int
            Maximum number of running workflows

        Returns
        -------
        entries : list[DbId]
            The entries that were launched
        """
        raise NotImplementedError()

    def validate(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Validated completed entries at a particular level

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are accepting

        Returns
        -------
        entries : list[DbId]
            The entries that were validated
        """
        raise NotImplementedError()

    def accept(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Accept completed entries at a particular level

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are accepting

        Returns
        -------
        entries : list[DbId]
            The entries that were accepted
        """
        raise NotImplementedError()

    def reject(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Reject entries at a particular level

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are rejecting

        Returns
        -------
        entries : list[DbId]
            The entries that were rejected
        """
        raise NotImplementedError()

    def rollback(self, level: LevelEnum, db_id: DbId, to_status: StatusEnum) -> list[DbId]:
        """Roll-backl entries at a particular level

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are rolling back

        to_status: StatusEnum
            The status we are rolling back to

        Returns
        -------
        entries : list[DbId]
            The entries that were rolled back
        """
        raise NotImplementedError()

    def fake_run(self, level: LevelEnum, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> None:
        """Pretend to run workflows, this is for testing

        Parameters
        ----------
        level: LevelEnum
           Selects which database table to search

        db_id : DbId
            Specifies the entries we are running

        status: StatusEnum
            Status value to set
        """
        raise NotImplementedError()

    def daemon(self, db_id: DbId, max_running: int = 100, sleep_time: int = 60, n_iter: int = -1) -> None:
        """Run a loop

        Parameters
        ----------
        db_id : DbId
            Specifies the campaign we are running against

        max_running : int
            Maximum number of running workflows

        sleep_time : int
            Time between cycles (in seconds)

        n_iter : int
            number of interations to run, -1 for no limit
        """
        raise NotImplementedError()
