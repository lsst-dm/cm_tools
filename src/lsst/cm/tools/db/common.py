from collections import OrderedDict
from typing import Any, Iterable, TextIO

from sqlalchemy import select, update
from sqlalchemy.orm import declarative_base

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import CMTableBase, ConfigBase, DbInterface, FragmentBase, ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.rollback import Rollback
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum


class SQLTableMixin:
    """Provides implementation of some common
    functions for Database tables
    """

    __allow_unmapped__ = True

    depend_: Iterable
    id: int | None

    def __init__(self, id: int) -> None:
        self.id = id

    @classmethod
    def insert_values(cls, dbi: DbInterface, **kwargs: Any) -> Any:
        """Inserts a new row at a given level with values given in kwargs"""
        conn = dbi.connection()
        new_entry = cls(**kwargs)
        conn.add(new_entry)
        return new_entry

    @classmethod
    def update_values(cls, dbi: DbInterface, row_id: int, **kwargs: Any) -> Any:
        """Updates a given row with values given in kwargs"""
        stmt = update(cls).where(cls.id == row_id).values(**kwargs)
        conn = dbi.connection()
        upd_result = conn.execute(stmt)
        check_result(upd_result)

    def check_prerequistes(self, dbi: DbInterface) -> bool:
        """Check the prerequisites of an entry"""
        for dep_ in self.depend_:
            entry = dbi.get_entry(dep_.db_id.level(), dep_.db_id)
            if entry.status.value < StatusEnum.accepted.value:
                return False
        return True

    def as_dict(self) -> dict[str, Any]:
        """Return row as a dict"""
        return OrderedDict([(c.name, getattr(self, c.name)) for c in self.__table__.columns])

    def print_full(self) -> None:
        """Print full row"""
        for k, v in self.as_dict().items():
            print(f"{k}: {v}")

    def print_formatted(self, stream: TextIO, fmt: str) -> None:
        stream.write(fmt.format(**self.__dict__))
        stream.write("\n")


class SQLScriptMixin(SQLTableMixin):
    """Provides implementation some functions
    needed for Script and Workflow objects
    """

    id: int | None
    frag_: FragmentBase | None
    status: StatusEnum

    def get_handler(self) -> Handler:
        """Return a Handler for this entry"""
        assert self.frag_ is not None
        return self.frag_.get_handler()

    @classmethod
    def check_status(cls, dbi: DbInterface, script: ScriptBase) -> StatusEnum:
        """Check the status of a script"""
        if script.checker is None:
            return script.status
        checker = Checker.get_checker(script.checker)
        if checker is None:
            return script.status
        new_values = checker.check_url(dbi, script)
        if new_values:
            stmt = update(cls).where(cls.id == script.id).values(**new_values)
            conn = dbi.connection()
            upd_result = conn.execute(stmt)
            check_result(upd_result)
        return script.status

    @classmethod
    def rollback_script(cls, dbi: DbInterface, entry: CMTableBase, script: ScriptBase) -> None:
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

    name: str | None
    frag_: FragmentBase | None
    config_: ConfigBase | None
    match_keys: list[str] = []
    parent_id: Any

    def get_handler(self) -> Handler:
        assert self.frag_
        return self.frag_.get_handler()

    def get_sub_handler(self, config_block: str) -> Handler:
        assert self.config_
        return self.config_.get_sub_handler(config_block)

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


def return_first_column(dbi: DbInterface, sel: Any) -> int | None:
    """Returns the first column in the first row matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    try:
        return sel_result.all()[0][0]
    except IndexError:
        return None


def print_select(dbi: DbInterface, stream: TextIO, sel: Any, fmt: str | None) -> None:
    """Prints all the rows matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    for row in sel_result:
        if fmt is None:
            stream.write(f"{str(row)}\n")
        else:
            row[0].print_formatted(stream, fmt)
