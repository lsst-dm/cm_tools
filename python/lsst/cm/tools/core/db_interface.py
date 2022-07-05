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

from typing import TextIO, Any
from collections.abc import Iterable
from dataclasses import dataclass

from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum


@dataclass
class DbId:
    """Information to identify a single entry in the CM database tables

    This consist of primary keys into each of the tables
    """

    p_id: int = None
    c_id: int = None
    s_id: int = None
    g_id: int = None
    w_id: int = None

    @classmethod
    def create_from_row(
            cls,
            row) -> DbId:
        ids = {}
        for key in ['p_id', 'c_id', 's_id', 'g_id', 'w_id']:
            try:
                ids[key] = row[key]
            except KeyError:
                ids[key] = None
        return cls(**ids)

    def to_tuple(self) -> tuple:
        """Return data as tuple"""
        return (self.p_id, self.c_id, self.s_id, self.g_id, self.w_id)

    def __getitem__(
            self,
            level: LevelEnum) -> int:
        """Return primary key at a particular level"""
        return self.to_tuple()[level.value]

    def extend(
            self,
            level: LevelEnum,
            row_id: int) -> DbId:
        """Return an extension of this DbId

        This adds a primary key at a particular level

        Parameters
        ----------
        level : LevelEnum
            The level were are adding the key at

        row_id : int
            The primary key we are adding

        Returns
        -------
        new_id : DbId
            The extended DbId
        """
        if level == LevelEnum.production:
            return DbId(p_id=row_id)
        if level == LevelEnum.campaign:
            return DbId(p_id=self.p_id, c_id=row_id)
        if level == LevelEnum.step:
            return DbId(p_id=self.p_id, c_id=self.c_id, s_id=row_id)
        if level == LevelEnum.group:
            return DbId(p_id=self.p_id, c_id=self.c_id, s_id=self.s_id, g_id=row_id)
        if level == LevelEnum.workflow:
            return DbId(p_id=self.p_id, c_id=self.c_id, s_id=self.s_id, g_id=self.g_id, w_id=row_id)
        raise RuntimeError("Bad level {level}")


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
    def full_name(
            cls,
            level: LevelEnum,
            **kwargs) -> str:
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

    def get_db_id(
            self,
            level: LevelEnum,
            **kwargs) -> DbId:
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

    def get_row_id(
            self,
            level: LevelEnum,
            **kwargs) -> int:
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

    def get_status(
            self,
            level: LevelEnum,
            db_id: DbId) -> StatusEnum:
        """Print a database entry or entries

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

    def print_(
            self,
            stream: TextIO,
            level: LevelEnum,
            db_id: DbId) -> None:
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

    def print_table(
            self,
            stream: TextIO,
            level: LevelEnum) -> None:
        """Print a database table

        Parameters
        ----------
        stream : TextIO
            The stream we will print to

        level: LevelEnum
            Selects which database table to print
        """
        raise NotImplementedError()

    def count(
            self,
            level: LevelEnum,
            db_id: DbId) -> int:
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

    def update(
            self,
            level: LevelEnum,
            db_id: DbId,
            **kwargs) -> None:
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

    def check(
            self,
            level: LevelEnum,
            db_id: DbId,
            recurse: bool = False) -> None:
        """Check all database entries at a particular level

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Selects which entries to check

        recurse : bool
            If true, will recursively check childern
        """
        raise NotImplementedError()

    def get_data(
            self,
            level: LevelEnum,
            db_id: DbId):
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

    def get_iterable(
            self,
            level: LevelEnum,
            db_id: DbId) -> Iterable:
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

    def insert(
            self,
            level: LevelEnum,
            parent_db_id: DbId,
            handler: Handler,
            recurse: bool = True,
            **kwargs) -> dict[str, Any]:
        """Insert a new database entry at a particular level

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        parent_db_id : DbId
            Specifies the parent entry to the entry we are inserting

        recurse : bool=True
            If true, call `handler.post_insert_hook` to recursive insert
            child entries

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

    def prepare(
            self,
            level: LevelEnum,
            db_id: DbId,
            recurse: bool = True,
            **kwargs) -> None:
        """Preparing a database entry for execution

        Parameters
        ----------
        level : LevelEnum
            Selects which database table to search

        db_id : DbId
            Database ID for this entry

        recurse : bool=True
            If true, allows prepared entries to insert new entries

        Keywords
        --------
        Keywords can be used in recursion
        """
        raise NotImplementedError()

    def queue_workflows(
            self,
            level: LevelEnum,
            db_id: DbId) -> None:
        """Queue all of the ready workflows matching the selection

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are queuing
        """
        raise NotImplementedError()

    def launch_workflows(
            self,
            level: LevelEnum,
            db_id: DbId,
            max_running: int) -> None:
        """Queue all of the ready workflows matching the selection

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are queuing
        """
        raise NotImplementedError()

    def accept(
            self,
            level: LevelEnum,
            db_id: DbId) -> None:
        """Accept all of the completed or part_fail
        entries at a particular level

        Parameters
        ----------
        level: LevelEnum
            Selects which database table to search

        db_id : DbId
            Specifies the entries we are accepting
        """
        raise NotImplementedError()
