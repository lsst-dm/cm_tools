from typing import Any, Iterable, Optional, TextIO

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import CMTableBase, DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.rollback import Rollback
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from sqlalchemy import and_, func, select, update
from sqlalchemy.orm import declarative_base

update_field_list = ["handler", "config_yaml"]
update_common_fields = [
    "data_query",
    "coll_source",
    "coll_in",
    "coll_out",
    "input_type",
    "output_type",
    "status",
    "superseeded",
]
update_script_fields = [
    "prepare_id",
    "collect_id",
]


class SQLTableMixin:

    depend_: Iterable
    id: Optional[int] = None

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
        for dep_ in self.depend_:
            entry = dbi.get_entry(dep_.db_id.level(), dep_.db_id)
            if entry.status.value < StatusEnum.accepted.value:
                return False
        return True


class SQLScriptMixin(SQLTableMixin):

    status: StatusEnum

    @classmethod
    def check_status(cls, dbi: DbInterface, entry: Any) -> StatusEnum:
        current_status = entry.status
        checker = Checker.get_checker(entry.checker)
        new_status = checker.check_url(entry.log_url, entry.status)
        if new_status != current_status:
            stmt = update(cls).where(cls.id == entry.id).values(status=new_status)
            conn = dbi.connection()
            upd_result = conn.execute(stmt)
            check_result(upd_result)
            conn.commit()
        return new_status

    @classmethod
    def rollback_script(cls, dbi: DbInterface, entry: Any) -> None:
        """Rollback a script"""
        rollback_handler = Rollback.get_rollback(entry.rollback)
        rollback_handler.rollback_script(entry)
        cls.update_values(dbi, entry.id, superseeded=True)

    @classmethod
    def get_rows_with_status_query(cls, status: StatusEnum) -> Any:
        """Returns the selection for all rows with a particular status"""
        sel = select([cls.id]).where(cls.status == status)
        return sel


class CMTable(SQLTableMixin, CMTableBase):

    level = LevelEnum.production

    name: Optional[str]
    handler: Optional[str]
    config_yaml: Optional[str]
    match_keys: list[str] = []
    parent_id: Optional[Any]

    def get_handler(self) -> Handler:
        return Handler.get_handler(self.handler, self.config_yaml)

    @classmethod
    def get_count_query(cls, db_id: Optional[DbId]) -> Any:
        """Return the query to count rows matching an id"""
        count_key = cls.parent_id
        if count_key is None:
            return func.count(cls.id)
        if db_id is not None:
            return func.count(count_key == db_id[cls.level])
        return func.count(count_key)

    @classmethod
    def get_row_query(cls, db_id: DbId, columns: list[Any]) -> Any:
        """Returns the selection a single row given db_id"""
        if not columns:
            sel = select(cls).where(cls.id == db_id[cls.level])
        else:
            sel = select(columns).where(cls.id == db_id[cls.level])
        return sel

    @classmethod
    def get_id_match_query(cls, parent_id: Optional[int], match_name: Any) -> Any:
        """Returns the selection to match a particular ID"""
        parent_field = cls.parent_id
        if parent_field is None:
            sel = select([cls.id]).where(cls.name == match_name)
        else:
            sel = select([cls.id]).where(and_(parent_field == parent_id, cls.name == match_name))
        return sel

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


def return_select_count(dbi: DbInterface, sel: Any) -> int:
    """Counts an iterable matching a selection"""
    conn = dbi.connection()
    count = func.count(sel)
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


def return_single_row(dbi: DbInterface, sel: Any) -> Any:
    """Returns the first row matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    return sel_result.all()[0]


def print_select(dbi: DbInterface, stream: TextIO, sel: Any) -> None:
    """Prints all the rows matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    for row in sel_result:
        stream.write(f"{str(row)}\n")
