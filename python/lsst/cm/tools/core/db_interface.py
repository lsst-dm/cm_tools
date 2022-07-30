from __future__ import annotations

from typing import Any, TextIO

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum


class TableBase:
    """Base class for database table interface

    Provided interface to insert and update entries
    """

    @classmethod
    def insert_values(cls, dbi: DbInterface, **kwargs: Any) -> Any:
        """Insert a new entry to a table

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        Keywords
        --------
        Give the values to insert

        Returns
        -------
        new_entry : Any
            The newly inserted entry
        """
        raise NotImplementedError()

    @classmethod
    def update_values(cls, dbi: DbInterface, row_id: int, **kwargs: Any) -> None:
        """Update the values in an entry

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database

        row_id : int
            The id of the entry we are updating

        Keywords
        --------
        Give the values to update

        """
        raise NotImplementedError()


class ScriptBase(TableBase):
    """Interface class for database entries describing Scripts and Jobs

    Scripts are command that run locally, but potentially
    asynchronously.

    Jobs are commands that run on a batch system

    This will require the derived class to implement
    a `check_status` method to check on the status of the script
    and a `rollback_script` method to clean up failed scripts
    """

    script_url = ""
    log_url = ""
    id = -1

    @classmethod
    def check_status(cls, dbi: DbInterface, entry: ScriptBase) -> StatusEnum:
        """Check the status of a script

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database

        entry : ScriptBase
            The entry in question

        Returns
        -------
        status : StatusEnum
            Status of the script in question
        """
        raise NotImplementedError()

    @classmethod
    def rollback_script(cls, dbi: DbInterface, entry: Any, script: ScriptBase) -> None:
        """Called when a particular entry is rejected

        Calling this function will result in the script
        being marked as `superseded`, and be ignored by further processing.

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        entry : Any
            The entry associated to the script

        script : ScriptBase
            The script we are rolling back
        """
        raise NotImplementedError()


class JobBase(ScriptBase):
    """Interface class for database entries describing Jobs

    This will require the derived class to implement
    a `check_status` method to check on the status of the script
    and a `rollback_job` method to clean up failed jobs
    """


class DependencyBase:
    """Interface class for database entries describing Dependencies"""

    @classmethod
    def add_prerequisite(cls, dbi: DbInterface, depend_id: DbId, prereq_id: DbId) -> DependencyBase:
        """Add a Dependency

        This will prevent depend_id from running until
        prereq_id is accepted

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database

        depend_id : DbId
            DbId of the dependent entry

        prereq_id : DbId
            DbId of the prerequisite entry

        Returns
        -------
        depend : DependencyBase
            The newly create dependency
        """
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

    def get_entry_from_fullname(self, level: LevelEnum, fullname: str) -> CMTableBase:
        """Return a selected entry

        Parameters
        ----------
        level : LevelEnum
            Specifies which database table to search

        fullname : str
            Full name of the entry

        Returns
        -------
        entry : CMTableBase
            Selected entry

        Notes
        -----
        This will return the entry that matches level and fullname
        """
        raise NotImplementedError()

    def get_entry_from_parent(self, parent_id: DbId, entry_name: str) -> CMTableBase:
        """Return a selected entry

        Parameters
        ----------
        parent_id: DbId
            Parent DbId

        entry_name: str
            Name for the entry in question

        Returns
        -------
        entry : CMTableBase
            Selected entry

        Notes
        -----
        This will return the entry that matches parent_id and entry_name
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

    def print_(self, stream: TextIO, level: LevelEnum, db_id: DbId) -> None:
        """Print a database entry or entries

        Parameters
        ----------
        stream : TextIO
            The stream we will print to

        level: LevelEnum
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

    def print_tree(self, stream: TextIO, level: LevelEnum, db_id: DbId) -> None:
        """Print a database table from a given entry
        in a tree-like format

        Parameters
        ----------
        stream : TextIO
            The stream we will print to

        level: LevelEnum
            Selects which database table to start with

        db_id: DbId
            Database ID specifying which entries to print.
            See class notes above.
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

    def insert(self, parent_db_id: DbId, handler: Handler, **kwargs: Any) -> CMTableBase:
        """Insert a new database entry at a particular level

        Parameters
        ----------
        parent_db_id : DbId
            Specifies the parent entry to the entry we are inserting

        handler : Handler
            The callback handler for the entry we are inserting

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

    def queue_jobs(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Queue all the ready jobs matching the selection

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

    def launch_jobs(self, level: LevelEnum, db_id: DbId, max_running: int) -> list[DbId]:
        """Launch all the pending jobs matching the selection

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are queuing

        max_running: int
            Maximum number of running jobs

        Returns
        -------
        entries : list[DbId]
            The entries that were launched
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

    def supersede(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Mark entries as superseded so that they will be ignored
        in subsequent processing

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are rolling back

        Returns
        -------
        entries : list[DbId]
            The entries that were marked
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
