# This file is part of cm_tools
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations  # Needed for class member returning class

from collections.abc import Iterable
from typing import Any, Optional, TextIO

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum


class ScriptBase:

    def check_status(self, conn) -> StatusEnum:
        raise NotImplementedError()


class CMTableBase:

    def get_handler(self) -> Handler:
        raise NotImplementedError()

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        raise NotImplementedError()


class DbInterface:
    """Base class for database interface

    Many of the interface function here take a DbId an an argument.
    This can specify either a single database entry, or all of the
    entries in a given table with a particular parent.

    In short, depending on the function, the user either must provide
    either a DbId specifying a single entry, or they use may provide
    DbId specifying multiple entries from a particular table.

    Internally, the DbInterface will use DbId to select entries for the
    requested operation.
    """

    @classmethod
    def full_name(cls, level: LevelEnum, **kwargs) -> str:
        """Utility function to return a full name
        associated to a database entry

        Parameters
        ----------
        level : LevelEnum
            Specifies which table we are refering to

        Keywords
        --------
        These are used to build the full name, see class notes.

        Returns
        -------
        fullname : str
            Unique string desribing this database entry
        """
        raise NotImplementedError()

    def get_repo(self, db_id: DbId) -> str:
        """Return the data repository for a given campaign

        Parameters
        ----------
        db_id : DbId
            The database ID used to identify the campaign

        Returns
        -------
        repo : str
            Url for the data repository
        """
        raise NotImplementedError()

    def get_prod_base(self, db_id: DbId) -> str:
        """Return the URL for the production area for a given campaign

        Parameters
        ----------
        db_id : DbId
            The database ID used to identify the campaign

        Returns
        -------
        repo : str
            Url for the root of the production area
        """
        raise NotImplementedError()

    def get_db_id(self, level: LevelEnum, **kwargs) -> DbId:
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
            The request database ID
        """
        raise NotImplementedError()

    def get_row_id(self, level: LevelEnum, **kwargs) -> int:
        """Return the primary key of a particular row in the database

         Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        Keywords
        --------
        These are used to sub-select a single matching row
        in the selected table.
        See class notes above.

        Returns
        -------
        row_id : int
            The primary key of the selected row
        """
        db_id = self.get_db_id(level, **kwargs)
        return db_id[level]

    def get_status(self, level: LevelEnum, db_id: DbId) -> StatusEnum:
        """Return the status of a selected entry

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Database ID specifying which row to select.
            See class notes above.

        Returns
        -------
        status : StatusEnum
            Status of the selected entry
        """
        raise NotImplementedError()

    def get_prerequisites(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        """Return the prerequisites of a selected entry

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Database ID specifying which row to select.
            See class notes above.

        Returns
        -------
        prerequisites : list[DbId]
            Prerequisites for the selected entry
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

    def print_(self, stream: TextIO, level: LevelEnum, db_id: DbId) -> None:
        """Print a database entry or entries

        Parameters
        ----------
        stream : TextIO
            The stream we will print to

        level: LevelEnum
            Selects which database table to print from

        db_id: DbId
            Database ID specifying which entries to print.
            See class notes above.
        """
        raise NotImplementedError()

    def print_table(self, stream: TextIO, level: LevelEnum) -> None:
        """Print a database table

        Parameters
        ----------
        stream : TextIO
            The stream we will print to

        level: LevelEnum
            Selects which database table to print
        """
        raise NotImplementedError()

    def count(self, level: LevelEnum, db_id: Optional[DbId]) -> int:
        """Count the number of database entries matching conditions

        Parameters
        ----------
        level: LevelEnum
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

    def update(self, level: LevelEnum, db_id: DbId, **kwargs) -> None:
        """Update a particular database entry

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id: DbId
            Database ID specifying which entry to update.
            See class notes above.

        Keywords
        --------
        These are passed to the handler which can use
        them to derive the values for the fields to update.
        """
        raise NotImplementedError()

    def check(self, level: LevelEnum, db_id: DbId, recurse: bool = False, counter: int = 2) -> None:
        """Check all database entries at a particular level

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Selects which entries to check

        recurse : bool
            If true, will recursively check childern

        counter : int
            Number of times to run check
        """
        raise NotImplementedError()

    def get_data(self, level: LevelEnum, db_id: DbId):
        """Return data in matching database entries

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Selects which entries to return

        Returns
        -------
        data : ???
            The matching data
        """
        raise NotImplementedError()

    def get_iterable(self, level: LevelEnum, db_id: DbId) -> Iterable:
        """Return an iterator over the matching database entries

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Selects which entries to include

        Returns
        -------
        itr : iterator
            Iterator over the matching rows
        """
        raise NotImplementedError()

    def add_prerequisite(self, depend_id: DbId, prereq_id: DbId) -> None:
        """Add a prerequisite to

        Parameters
        ----------
        depend_id : DbId
            The dependent entry

        prereq_id : DbId
            The prerequiste entry
        """
        raise NotImplementedError()

    def add_script(
        self,
        script_url: Optional[str] = None,
        log_url: Optional[str] = None,
        checker: Optional[str] = None,
        status: Optional[StatusEnum] = StatusEnum.ready,
    ) -> int:
        """Insert a new row with details about a script

        Parameters
        ----------
        script_url: str
            The location of the script

        log_url: str
            The location of the log
            (which can be checked to set the script status)

        checker : str
            Fully defined path to a class to check the status of the script

        status : StatusEnum
            Status of the script

        Returns
        -------
        script_id : int
            The ID for the new script
        """
        raise NotImplementedError()

    def insert(
        self, level: LevelEnum, parent_db_id: DbId, handler: Handler, **kwargs
    ) -> dict[str, Any]:
        """Insert a new database entry at a particular level

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        parent_db_id : DbId
            Specifies the parent entry to the entry we are inserting

        Keywords
        --------
        These are used to select the parent rows to the
        entry being inserted.
        See class notes above.

        Returns
        -------
        insert_fields : dict[str, Any]:
            The keys and values being inserted into the new entry
        """
        raise NotImplementedError()

    def prepare(self, level: LevelEnum, db_id: DbId, **kwargs) -> list[DbId]:
        """Preparing a database entry for execution

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
,         Keywords can be used in recursion
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

    def accept(self, level: LevelEnum, db_id: DbId, recurse: bool = True) -> list[DbId]:
        """Accept all the completed or part_fail
        entries at a particular level

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
        """Reject all the completed or part_fail
        entries at a particular level

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are accepting

        Returns
        -------
        entries : list[DbId]
            The entries that were rejected
        """
        raise NotImplementedError()

    def fake_run(self, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> None:
        """Pretend to run workflows, this is for testing

        Parameters
        ----------
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
