from typing import Any, Iterable, Optional, TextIO

from sqlalchemy import func, select, update
from sqlalchemy.orm import declarative_base

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import CMTableBase, DbInterface, TableBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.rollback import Rollback
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum


class SQLTableMixin:
    """Provides implementation of some common
    functions for Database tables
    """

    depend_: Iterable
    id: Optional[int]

    @classmethod
    def insert_values(cls, dbi: DbInterface, **kwargs: Any) -> Any:
        """Inserts a new row at a given level with values given in kwargs"""
        counter = func.count(cls.id)
        conn = dbi.connection()
        next_id = return_count(conn, counter) + 1
        new_entry = cls(id=next_id, **kwargs)
        conn.add(new_entry)
        conn.commit()
        return new_entry

    @classmethod
    def update_values(cls, dbi: DbInterface, row_id: int, **kwargs: Any) -> Any:
        """Updates a given row with values given in kwargs"""
        stmt = update(cls).where(cls.id == row_id).values(**kwargs)
        conn = dbi.connection()
        upd_result = conn.execute(stmt)
        check_result(upd_result)
        conn.commit()

    def check_prerequistes(self, dbi: DbInterface) -> bool:
        """Check the prerequisites of an entry"""
        for dep_ in self.depend_:
            entry = dbi.get_entry(dep_.db_id.level(), dep_.db_id)
            if entry.status.value < StatusEnum.accepted.value:
                return False
        return True


class SQLScriptMixin(SQLTableMixin):
    """Provides implementation some functions
    need for Script and Workflow objects
    """

    id: Optional[int]
    handler: Optional[str]
    config_yaml: Optional[str]
    status: StatusEnum

    def get_handler(self) -> Handler:
        """Return a Handler for this entry"""
        return Handler.get_handler(self.handler, self.config_yaml)

    @classmethod
    def check_status(cls, dbi: DbInterface, entry: Any) -> StatusEnum:
        """Check the status of a script"""
        current_status = entry.status
        checker = Checker.get_checker(entry.checker)
        new_status = checker.check_url(entry.stamp_url, entry.status)
        if new_status != current_status:
            stmt = update(cls).where(cls.id == entry.id).values(status=new_status)
            conn = dbi.connection()
            upd_result = conn.execute(stmt)
            check_result(upd_result)
            conn.commit()
        return new_status

    @classmethod
    def rollback_script(cls, dbi: DbInterface, entry: Any, script: TableBase) -> None:
        """Rollback a script"""
        rollback_handler = Rollback.get_rollback(script.rollback)
        rollback_handler.rollback_script(entry, script)
        cls.update_values(dbi, entry.id, superseded=True)


class CMTable(SQLTableMixin, CMTableBase):
    """Base class for database entries
    that represent data processing elemements
    such as `Production`, `Campaign` etc...
    """

    level = LevelEnum.production

    name: Optional[str]
    handler: Optional[str]
    config_yaml: Optional[str]
    match_keys: list[str] = []
    parent_id: Optional[Any]

    def get_handler(self) -> Handler:
        return Handler.get_handler(self.handler, self.config_yaml)

    @classmethod
    def get_match_query(cls, db_id: DbId) -> Any:
        """Returns the selection all rows given db_id at a given level"""
        if db_id is None:
            id_tuple: tuple = ()
        else:
            id_tuple = db_id.to_tuple()[0 : cls.level.value + 1]
        parent_key = None
        row_id = None
        for i, row_id_ in enumerate(id_tuple):
            if row_id_ is not None:
                parent_key = cls.match_keys[i]
                row_id = row_id_
        if parent_key is None:
            sel = select(cls)
        else:
            sel = select(cls).where(parent_key == row_id)
        return sel


Base = declarative_base()


def check_result(result: Any) -> None:
    """Placeholder function to check on SQL query results"""
    assert result


def return_count(dbi: DbInterface, count: Any) -> int:
    """Returns the number of rows mathcing a selection"""
    conn = dbi.connection()
    count_result = conn.execute(count)
    check_result(count_result)
    return count_result.scalar()


def return_first_column(dbi: DbInterface, sel: Any) -> Optional[int]:
    """Returns the first column in the first row matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    try:
        return sel_result.all()[0][0]
    except IndexError:
        return None


def print_select(dbi: DbInterface, stream: TextIO, sel: Any) -> None:
    """Prints all the rows matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    for row in sel_result:
        stream.write(f"{str(row)}\n")
