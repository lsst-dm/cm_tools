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
    def insert_values(cls, dbi: DbInterface, **kwargs: Any) -> TableBase:
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
        new_entry : TableBase
            Newly inserted entry
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
            Id of the entry we are updating

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
    stamp_url = ""
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
            Entry in question

        Returns
        -------
        status : StatusEnum
            Status of the script in question
        """
        raise NotImplementedError()

    @classmethod
    def rollback_script(cls, dbi: DbInterface, entry: CMTableBase, script: ScriptBase) -> None:
        """Called when a particular entry is rejected

        Calling this function will result in the script
        being marked as `superseded`, and be ignored by further processing.

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        entry : CMTableBase
            Entry associated to the script

        script : ScriptBase
            Script we are rolling back
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
            Newly create dependency
        """
        raise NotImplementedError()


class FragmentBase(TableBase):
    """Interface class for configuration fragments"""

    def get_handler(self) -> Handler:
        """Get the handler associated to this Fragment
        Parameters
        ----------
        dbi : DbInterface
            Interface to the database

        Returns
        -------
        handler : Handler
            The associated handler
        """
        raise NotImplementedError()


class ConfigBase(TableBase):
    """Interface class for configurations"""

    def get_sub_handler(self, config_block: str) -> Handler:
        """Get the handler to a sub-fragment

        Parameters
        ----------
        config_block : str
            The tag that identifies block of the configuration

        Returns
        -------
        handler : Handler
            The associated handler
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

    def get_sub_handler(self, config_block: str) -> Handler:
        """Return the associated callback `Handler` for a child node"""
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

    def __init__(self) -> None:
        Handler.handler_cache.clear()

    def connection(self) -> Any:
        """Return the database connection object"""
        raise NotImplementedError()

    def get_db_id(self, **kwargs: Any) -> DbId:
        """Return an id that identifies one or more database entries

        Parameters
        ----------
        kwargs : Any
            These are used to specify the entry in question

        Notes
        -----
        This will first check if `fullname` is present in kwargs.
        If so, it will parse fullname to identify which Table to search
        and then search for a matching `fullname`.

        If `fullname` is not present this will search for
        production_name, campaign_name, step_name, group_name and workflow_idx
        and use those to identify which Table to search.

        Returns
        -------
        db_id : DbId
            Requested database ID
        """
        raise NotImplementedError()

    def get_entry_from_fullname(self, fullname: str) -> CMTableBase:
        """Return a selected entry

        Parameters
        ----------
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

    def get_config(self, config_name: str) -> ConfigBase:
        """Return a selected configuration object

        Parameters
        ----------
        config_name : str
            Selects which configutaion

        Returns
        -------
        config : ConfigBase
            Selected configuration
        """
        raise NotImplementedError()

    def print_(self, stream: TextIO, level: LevelEnum, db_id: DbId, fmt: str | None = None) -> None:
        """Print a database entry or entries

        Parameters
        ----------
        stream : TextIO
            Stream we will print to

        level: LevelEnum
            Selects which database table to print

        db_id: DbId
            Database ID specifying which entries to print.
            See class notes above.

        fmt: str | None
            If provided, format for printing
        """
        raise NotImplementedError()

    def print_table(self, stream: TextIO, which_table: TableEnum, **kwargs: Any) -> None:
        """Print a database table

        Parameters
        ----------
        stream : TextIO
            Stream we will print to

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
            Stream we will print to

        level: LevelEnum
            Selects which database table to start with

        db_id: DbId
            Database ID specifying which entries to print.
            See class notes above.
        """
        raise NotImplementedError()

    def print_config(self, stream: TextIO, config_name: str) -> None:
        """Print a information about a given configuration

        Parameters
        ----------
        stream : TextIO
            Stream we will print to

        config_name : str
            Name of the configuration in question
        """
        raise NotImplementedError()

    def summarize_output(self, stream: TextIO, level: LevelEnum, db_id: DbId) -> None:
        """Print a summary of the outputs associated to a particular entry

        Parameters
        ----------
        stream : TextIO
            Stream we will print to

        level: LevelEnum
            Selects which database table to start with

        db_id: DbId
            Database ID specifying which entries to print.
            See class notes above.
        """
        raise NotImplementedError()

    def associate_kludge(self, level: LevelEnum, db_id: DbId) -> None:
        """Run a kludged version of bulter associate

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to start with

        db_id: DbId
            Database ID specifying which entries to use.
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

    def insert(
        self,
        parent_db_id: DbId,
        config_block: str,
        config: ConfigBase | None,
        **kwargs: Any,
    ) -> CMTableBase:
        """Insert a new database entry at a particular level

        Parameters
        ----------
        parent_db_id : DbId
            Specifies the parent entry to the entry we are inserting

        config_block: str
            Specifics which part of the configuration to use for this entry

        config : ConfigBase
            Configuration associated to this entry

        kwargs : Any
            These can be used to override configuration values

        Returns
        -------
        new_entry : CMTableBase
            Newly inserted entry
        """
        raise NotImplementedError()

    def insert_rescue(
        self,
        db_id: DbId,
        config_block: str,
        **kwargs: Any,
    ) -> CMTableBase:
        """Insert a new database entry at a particular level

        Parameters
        ----------
        db_id : DbId
            Specifies the original version of the entry we are inserting

        config_block: str
            Specifics which part of the configuration to use for this entry

        kwargs : Any
            These can be used to override configuration values

        Returns
        -------
        new_entry : CMTableBase
            Newly inserted entry
        """
        raise NotImplementedError()

    def add_script(
        self,
        parent_db_id: DbId,
        script_name: str,
        config: ConfigBase | None = None,
        **kwargs: Any,
    ) -> ScriptBase:
        """Insert a new script

        Parameters
        ----------
        parent_db_id : DbId
            Specifies the parent entry to the script we are inserting

        script_name: str
            Name of the script, also specifies configuration block

        config : ConfigBase
            Configuration associated to this entry

        kwargs : Any
            These can be used to override configuration values

        Returns
        -------
        new_script : ScriptBase
            Newly inserted script
        """
        raise NotImplementedError()

    def add_job(
        self,
        parent_db_id: DbId,
        job_name: str,
        config: ConfigBase | None = None,
        **kwargs: Any,
    ) -> JobBase:
        """Insert a new script

        Parameters
        ----------
        parent_db_id : DbId
            Specifies the parent entry to the script we are inserting

        job_name: str
            Name of the job, also specifies configuration block

        config : ConfigBase
            Configuration associated to this entry

        kwargs : Any
            These can be used to override configuration values

        Returns
        -------
        new_job : JobBase
            Newly inserted job
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
            Entries that were queued
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
            Entries that were launched
        """
        raise NotImplementedError()

    def requeue_jobs(
        self,
        level: LevelEnum,
        db_id: DbId,
    ) -> list[DbId]:
        """Requeue all the failed jobs matching the selection

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are queuing

        Returns
        -------
        entries : list[DbId]
            Entries that were launched
        """
        raise NotImplementedError()

    def rerun_scripts(
        self,
        level: LevelEnum,
        db_id: DbId,
        script_name: str,
    ) -> list[DbId]:
        """Re-run all the failed scripts matching the selection

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are queuing

        script_name : str
            Name of the script in question

        Returns
        -------
        entries : list[DbId]
            Entries that were launched
        """
        raise NotImplementedError()

    def accept(self, level: LevelEnum, db_id: DbId, rescuable: bool = False) -> list[DbId]:
        """Accept completed entries at a particular level

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are accepting

        rescuable: bool
            Mark the entries as rescuable instead of accepted

        Returns
        -------
        entries : list[DbId]
            Entries that were accepted
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
            Entries that were rejected
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
            Status we are rolling back to

        Returns
        -------
        entries : list[DbId]
            Entries that were rolled back
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
            Specifies the entries we are superseding

        Returns
        -------
        entries : list[DbId]
            Entries that were marked
        """
        raise NotImplementedError()

    def fake_run(self, level: LevelEnum, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> list[int]:
        """Pretend to run workflows, this is for testing

        Parameters
        ----------
        level: LevelEnum
           Selects which database table to search

        db_id : DbId
            Specifies the entries we are running

        status: StatusEnum
            Status value to set

        Returns
        -------
        db_id_list : list[int]
            Ids of the workflows that were affected
        """
        raise NotImplementedError()

    def fake_script(
        self, level: LevelEnum, db_id: DbId, script_name: str, status: StatusEnum = StatusEnum.completed
    ) -> list[int]:
        """Pretend to run scripts, this is for testing

        Parameters
        ----------
        level: LevelEnum
           Selects which database table to search

        db_id : DbId
            Specifies the entries we are running

        script_name : str
            Specifies which types of scripts to fake

        status: StatusEnum
            Status value to set

        Returns
        -------
        db_id_list : list[DbId]
            Ids of the scripts that were affected
        """
        raise NotImplementedError()

    def set_status(self, level: LevelEnum, db_id: DbId, status: StatusEnum) -> None:
        """Set the status of an entry

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

    def set_job_status(
        self, level: LevelEnum, db_id: DbId, script_name: str, status: StatusEnum = StatusEnum.completed
    ) -> None:
        """Set the status of jobs

        Parameters
        ----------
        level: LevelEnum
           Selects which database table to search

        db_id : DbId
            Specifies the entries we are running

        script_name : str
            Specifies which types of scripts to set the status for

        status: StatusEnum
            Status value to set

        Returns
        -------
        db_id_list : list[DbId]
            Ids of the scripts that were affected
        """
        raise NotImplementedError()

    def set_script_status(
        self, level: LevelEnum, db_id: DbId, script_name: str, status: StatusEnum = StatusEnum.completed
    ) -> list[int]:
        """Set the status of scripts

        Parameters
        ----------
        level: LevelEnum
           Selects which database table to search

        db_id : DbId
            Specifies the entries we are running

        script_name : str
            Specifies which types of scripts to set the status for

        status: StatusEnum
            Status value to set

        Returns
        -------
        db_id_list : list[DbId]
            Ids of the scripts that were affected
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
            Number of interations to run, -1 for no limit
        """
        raise NotImplementedError()

    def parse_config(self, config_name: str, config_yaml: str) -> ConfigBase:
        """Parse a configuration file

        Parameters
        ----------
        config_name : str
            Name to give to the configuraiton

        config_yaml : str
            Path to the file with the configurations

        Returns
        -------
        config : ConfigBase
            The configuration object
        """
        raise NotImplementedError()

    def load_error_types(self, config_yaml: str) -> None:
        """Parse a configuration file to load error types

        Parameters
        ----------
        config_yaml : str
            Path to the file with the configurations

        Returns
        -------
        None
        """
        raise NotImplementedError()

    def match_error_type(self, panda_code: str, diag_message: str) -> Any:
        """Get the ErrorType associated to a particular error

        Parameters
        ----------
        panda_code : str
            Error code generated by PanDA

        diag_message: str
            Diagnostic error message

        Returns
        -------
        error_type : Any
            The type of error
        """
        raise NotImplementedError()

    def modify_error_type(self, error_name: str, **kwargs: Any) -> None:
        """put what it does before committing
        Parameters
        ----------
        error_name: str
            Unique name for this ErrorType

        Keywords
        --------
        Key-value pairs to modify

        Returns
        -------
        None
        """
        raise NotImplementedError()

    def rematch_errors(self) -> Any:
        """Rematch the error instances"""
        raise NotImplementedError()

    def extend_config(self, config_name: str, config_yaml: str) -> ConfigBase:
        """Parse a configuration file and add it to an existing configuration

        Parameters
        ----------
        config_name : str
            Name to give to the configuraiton

        config_yaml : str
            Path to the file with the configurations

        Returns
        -------
        config : ConfigBase
            The configuration object
        """
        raise NotImplementedError()

    def report_errors(self, stream: TextIO, level: LevelEnum, db_id: DbId) -> None:
        """Report the errors associated with a particular entry

        Parameters
        ----------
        stream : TextIO
            Stream we will print to

        level: LevelEnum
            Selects which database table to start with

        db_id: DbId
            Database ID specifying which entries to print.
            See class notes above.
        """
        raise NotImplementedError()

    def report_error_trend(self, stream: TextIO, error_name: str) -> None:
        """Report if errors have been seen in prior workflows and if so, when

        Parameters
        ----------
        stream : TextIO
            Stream we will print to

        error_name: str
            Unique name for this ErrorType
        """
        raise NotImplementedError()

    def commit_errors(self, job_id: int, errors_aggregate: Any) -> None:
        """Commit the errors associated with a particular job

        Parameters
        ----------
        job_id: int
            The job in question

        errors_aggregate: Any
            The set of errors
        """
        raise NotImplementedError()
