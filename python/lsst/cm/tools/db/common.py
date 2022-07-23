import os
from typing import Any, Iterable, Optional, TextIO

from lsst.cm.tools.core.db_interface import CMTableBase, DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, safe_makedirs
from sqlalchemy import and_, func, select, update  # type: ignore
from sqlalchemy.orm import declarative_base

update_field_list = ["handler", "config_yaml"]
update_common_fields = [
    "prepare_script",
    "collect_script",
    "data_query",
    "coll_source",
    "coll_in",
    "coll_out",
    "status",
]


class CMTable(CMTableBase):

    level = LevelEnum.production
    id = None
    name = None
    handler = None
    config_yaml = None
    status = None
    db_id = None
    fullname = None
    match_keys = []

    def get_handler(self) -> Handler:
        return Handler.get_handler(self.handler, self.config_yaml)

    def prepare(self, dbi: DbInterface, handler, **kwargs):
        """Called when preparing a database entry for execution

        Can be used to prepare additional entries, for example,
        the children of this entry.

        Can also be used to do any actions associated to preparing this entry,
        e.g., making TAGGED Butler collections

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        handler : Handler
            Callback handler

        Returns
        -------
        entries : list[DbId]
            The entries that were prepared

        Keywords
        --------
        Keywords can be used by sub-classes
        """
        db_id_list = []
        prod_base_url = dbi.get_prod_base(self.db_id)
        full_path = os.path.join(prod_base_url, self.fullname)
        safe_makedirs(full_path)
        db_id_list.append(self.db_id)
        update_kwargs = {}
        script = handler.prepare_script_hook(self.level, dbi, self)
        if script is not None:
            update_kwargs["prepare_script"] = script.id
        update_kwargs["status"] = StatusEnum.preparing
        if self.level == LevelEnum.step:
            db_id_list += handler.make_groups(dbi, self.db_id, self)
        elif self.level == LevelEnum.workflow:
            run_script = handler.workflow_script_hook(dbi, self, **kwargs)
            update_kwargs["run_script"] = run_script.id
        self.update_values(dbi, self.db_id, **update_kwargs)
        return db_id_list

    @classmethod
    def get_parent_key(cls):
        raise NotImplementedError()

    @classmethod
    def post_insert(cls, dbi: DbInterface, handler, new_entry: CMTableBase, **kwargs):
        return None

    @classmethod
    def get_count_query(cls, db_id: Optional[DbId]):
        """Return the query to count rows matching an id"""
        count_key = cls.get_parent_key()
        if count_key is None:
            return func.count(cls.id)
        if db_id is not None:
            return func.count(count_key == db_id[cls.level])
        return func.count(count_key)

    @classmethod
    def get_row_query(cls, db_id: DbId, columns=None):
        """Returns the selection a single row given db_id"""
        if columns is None:
            sel = select(cls).where(cls.id == db_id[cls.level])
        else:
            sel = select(columns).where(cls.id == db_id[cls.level])
        return sel

    @classmethod
    def get_rows_with_status_query(cls, status: StatusEnum):
        """Returns the selection for all rows with a particular status"""
        sel = select([cls.id]).where(cls.status == status)
        return sel

    @classmethod
    def get_id_match_query(cls, parent_id: Optional[int], match_name: Any):
        """Returns the selection to match a particular ID"""
        parent_field = cls.get_parent_key()
        if parent_field is None:
            sel = select([cls.id]).where(cls.name == match_name)
        else:
            sel = select([cls.id]).where(and_(parent_field == parent_id, cls.name == match_name))
        return sel

    @classmethod
    def get_match_query(cls, db_id: DbId):
        """Returns the selection all rows given db_id at a given level"""
        if db_id is None:
            id_tuple = ()
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

    @classmethod
    def insert_values(cls, dbi: DbInterface, **kwargs):
        """Inserts a new row at a given level with values given in kwargs"""
        counter = func.count(cls.id)
        conn = dbi.connection()
        next_id = return_count(conn, counter) + 1
        new_entry = cls(id=next_id, **kwargs)
        conn.add(new_entry)
        conn.commit()
        return new_entry

    @classmethod
    def update_values(cls, dbi: DbInterface, db_id: DbId, **kwargs):
        """Updates a given row with values given in kwargs"""
        stmt = update(cls).where(cls.id == db_id[cls.level]).values(**kwargs)
        conn = dbi.connection()
        upd_result = conn.execute(stmt)
        check_result(upd_result)
        conn.commit()


Base = declarative_base()


def check_result(result) -> None:
    """Placeholder function to check on SQL query results"""
    assert result


def return_count(dbi: DbInterface, count) -> int:
    """Returns the number of rows mathcing a selection"""
    conn = dbi.connection()
    count_result = conn.execute(count)
    check_result(count_result)
    return count_result.scalar()


def return_select_count(dbi: DbInterface, sel) -> int:
    """Counts an iterable matching a selection"""
    conn = dbi.connection()
    itr = return_iterable(conn, sel)
    n_sel = 0
    for _ in itr:
        n_sel += 1
    return n_sel


def return_first_column(dbi: DbInterface, sel) -> Optional[int]:
    """Returns the first column in the first row matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    try:
        return sel_result.all()[0][0]
    except IndexError:
        return None


def return_single_row(dbi: DbInterface, sel):
    """Returns the first row matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    return sel_result.all()[0]


def return_iterable(dbi: DbInterface, sel) -> Iterable:
    """Returns an iterable matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    for x_ in sel_result:
        yield x_[0]


def print_select(dbi: DbInterface, stream: TextIO, sel) -> None:
    """Prints all the rows matching a selection"""
    conn = dbi.connection()
    sel_result = conn.execute(sel)
    check_result(sel_result)
    for row in sel_result:
        stream.write(f"{str(row)}\n")
