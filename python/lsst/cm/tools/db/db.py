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

from typing import Any, Iterable, Optional, TextIO

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import (  # type: ignore
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    String,
    Table,
    and_,
    create_engine,
    func,
    select,
    update,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Script(Base):
    __tablename__ = "script"

    x_id = Column(Integer, primary_key=True)  # Unique script ID
    script_url = Column(String)  # Url for script
    log_url = Column(String)  # Url for log
    config_url = Column(String)  # Url for config
    checker = Column(String)  # Checker class
    status = Column(Enum(StatusEnum))  # Status flag


class Production(Base):
    __tablename__ = "production"

    p_id = Column(Integer, primary_key=True)  # Unique production ID
    name = Column(String, unique=True)  # Production Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file


class Campaign(Base):
    __tablename__ = "campaign"

    c_id = Column(Integer, primary_key=True)  # Unique campaign ID
    p_id = Column(Integer, ForeignKey(Production.p_id))
    fullname = Column(String, unique=True)  # Full name of this campaign
    c_name = Column(String)  # Campaign Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.x_id))
    collect_script = Column(Integer, ForeignKey(Script.x_id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    butler_repo = Column(String)  # URL for butler repository
    prod_base_url = Column(String)  # URL for root of the production area


class Step(Base):
    __tablename__ = "step"

    s_id = Column(Integer, primary_key=True)  # Unique Step ID
    p_id = Column(Integer, ForeignKey(Production.p_id))
    c_id = Column(Integer, ForeignKey(Campaign.c_id))
    fullname = Column(String, unique=True)  # Full name of this step
    s_name = Column(String)  # Step name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.x_id))
    collect_script = Column(Integer, ForeignKey(Script.x_id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    previous_step_id = Column(Integer)


class Group(Base):
    __tablename__ = "group"

    g_id = Column(Integer, primary_key=True)  # Unique Group ID
    p_id = Column(Integer, ForeignKey(Production.p_id))
    c_id = Column(Integer, ForeignKey(Campaign.c_id))
    s_id = Column(Integer, ForeignKey(Step.s_id))
    fullname = Column(String, unique=True)  # Full name of this group
    g_name = Column(String)  # Group name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.x_id))
    collect_script = Column(Integer, ForeignKey(Script.x_id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag


class Workflow(Base):
    __tablename__ = "workflow"

    w_id = Column(Integer, primary_key=True)  # Unique Workflow ID
    p_id = Column(Integer, ForeignKey(Production.p_id))
    c_id = Column(Integer, ForeignKey(Campaign.c_id))
    s_id = Column(Integer, ForeignKey(Step.s_id))
    g_id = Column(Integer, ForeignKey(Group.g_id))
    fullname = Column(String, unique=True)  # Full name of this workflow
    w_idx = Column(Integer)  # Index for this workflow
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.x_id))
    collect_script = Column(Integer, ForeignKey(Script.x_id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    n_tasks_all = Column(Integer, default=0)  # Number of associated tasks
    n_tasks_done = Column(Integer, default=0)  # Number of finished tasks
    n_tasks_failed = Column(Integer, default=0)  # Number of failed tasks
    n_clusters_all = Column(Integer, default=0)  # Number of associated clusters
    n_clusters_done = Column(Integer, default=0)  # Number of finished clusters
    n_clusters_failed = Column(Integer, default=0)  # Number of failed clusters
    workflow_start = Column(DateTime)  # Workflow start time
    workflow_end = Column(DateTime)  # Workflow end time
    workflow_cputime = Column(Float)
    run_script = Column(Integer, ForeignKey(Script.x_id))


class Dependency(Base):
    __tablename__ = "dependency"

    d_id = Column(Integer, primary_key=True)  # Unique dependency ID
    p_id = Column(Integer, ForeignKey(Production.p_id))
    c_id = Column(Integer, ForeignKey(Campaign.c_id))
    s_id = Column(Integer, ForeignKey(Step.s_id))
    g_id = Column(Integer, ForeignKey(Group.g_id))
    w_id = Column(Integer, ForeignKey(Workflow.w_id))
    depend_p_id = Column(Integer, ForeignKey(Production.p_id))
    depend_c_id = Column(Integer, ForeignKey(Campaign.c_id))
    depend_s_id = Column(Integer, ForeignKey(Step.s_id))
    depend_g_id = Column(Integer, ForeignKey(Group.g_id))
    depend_w_id = Column(Integer, ForeignKey(Workflow.w_id))


def create_db(engine) -> None:
    """Creates a database as specific by `engine.url`

    Populates the database with empty tables
    """
    from sqlalchemy_utils import create_database  # pylint: disable=import-outside-toplevel

    create_database(engine.url)
    Base.metadata.create_all(engine)


def build_engine(db_url, **kwargs):
    """Return the sqlalchemy engine, building the database if needed"""
    from sqlalchemy_utils import database_exists  # type: ignore

    kwcopy = kwargs.copy()
    create = kwcopy.pop("create", False)
    engine = create_engine(db_url, **kwcopy)
    if not database_exists(engine.url):
        if create:
            create_db(engine)
    if not database_exists(engine.url):
        raise RuntimeError(f"Failed to access or create database {db_url}")
    return engine


def get_table(level: LevelEnum) -> Table:
    """Return the Table corresponding to a `level`"""
    all_tables = {
        LevelEnum.production: Production,
        LevelEnum.campaign: Campaign,
        LevelEnum.step: Step,
        LevelEnum.group: Group,
        LevelEnum.workflow: Workflow,
    }
    return all_tables[level]


def get_primary_key(level: LevelEnum) -> Column:
    """Return the primary key in the table corresponding to a `level`"""
    all_keys = {
        LevelEnum.production: Production.p_id,
        LevelEnum.campaign: Campaign.c_id,
        LevelEnum.step: Step.s_id,
        LevelEnum.group: Group.g_id,
        LevelEnum.workflow: Workflow.w_id,
    }
    return all_keys[level]


def get_status_key(level: LevelEnum) -> Optional[Column]:
    """Return the primary key in the table corresponding to a `level`"""
    all_keys = {
        LevelEnum.production: None,
        LevelEnum.campaign: Campaign.status,
        LevelEnum.step: Step.status,
        LevelEnum.group: Group.status,
        LevelEnum.workflow: Workflow.status,
    }
    return all_keys[level]


def get_name_field(level: LevelEnum) -> Column:
    """Return the `name` field in a table corresponding to a `level`"""
    all_keys = {
        LevelEnum.production: Production.p_name,
        LevelEnum.campaign: Campaign.c_name,
        LevelEnum.step: Step.s_name,
        LevelEnum.group: Group.g_name,
        LevelEnum.workflow: Workflow.w_idx,
    }
    return all_keys[level]


def get_parent_field(level: LevelEnum) -> Optional[Column]:
    """Return the id field of the parent entry in a table
    corresponding to a `level`
    """
    all_keys = {
        LevelEnum.production: None,
        LevelEnum.campaign: Campaign.p_id,
        LevelEnum.step: Step.c_id,
        LevelEnum.group: Group.s_id,
        LevelEnum.workflow: Workflow.g_id,
    }
    return all_keys[level]


def get_matching_key(table_level: LevelEnum, match_level: LevelEnum) -> Column:
    """Return the id field that can be used to match all
    entries in a particular table with any level of parent
    """
    all_keys = {
        LevelEnum.production: [Production.p_id],
        LevelEnum.campaign: [Campaign.p_id, Campaign.c_id],
        LevelEnum.step: [Step.p_id, Step.c_id, Step.s_id],
        LevelEnum.group: [Group.p_id, Group.c_id, Group.s_id, Group.g_id],
        LevelEnum.workflow: [
            Workflow.p_id,
            Workflow.c_id,
            Workflow.s_id,
            Workflow.g_id,
            Workflow.w_id,
        ],
    }
    return all_keys[table_level][match_level.value]


def get_depend_key(level: LevelEnum):
    """Return the id field of the dependency entry
    corresponding to a `level`
    """
    all_keys = {
        LevelEnum.production: Dependency.depend_p_id,
        LevelEnum.campaign: Dependency.depend_c_id,
        LevelEnum.step: Dependency.depend_s_id,
        LevelEnum.group: Dependency.depend_g_id,
        LevelEnum.workflow: Dependency.depend_w_id,
    }
    return all_keys[level]


def get_update_field_list(level: LevelEnum) -> list[str]:
    """Return the list of fields that we can update
    in a particular table
    """
    field_list = ["handler", "config_yaml"]
    common_fields = [
        "prepare_script",
        "collect_script",
        "data_query",
        "coll_source",
        "coll_in",
        "coll_out",
    ]
    extra_fields: dict[LevelEnum, list[str]] = {
        LevelEnum.production: [],
        LevelEnum.campaign: common_fields,
        LevelEnum.step: common_fields,
        LevelEnum.group: common_fields,
        LevelEnum.workflow: common_fields
        + [
            "n_tasks_done",
            "n_tasks_failed",
            "n_clusters_done",
            "n_clusters_failed",
            "workflow_start",
            "workflow_end",
            "workflow_cputime",
            "run_script",
        ],
    }
    field_list += extra_fields[level]
    return field_list


def _check_result(result) -> None:
    """Placeholder function to check on SQL query results"""
    assert result


def return_first_column(conn, sel) -> Optional[int]:
    """Returns the first column in the first row matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    try:
        return sel_result.all()[0][0]
    except IndexError:
        return None


def return_single_row(conn, sel):
    """Returns the first row matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    return sel_result.all()[0]


def return_iterable(conn, sel) -> Iterable:
    """Returns an iterable matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    for x_ in sel_result:
        yield x_[0]


def return_count(conn, count) -> int:
    """Returns the number of rows mathcing a selection"""
    count_result = conn.execute(count)
    _check_result(count_result)
    return count_result.scalar()


def return_select_count(conn, sel) -> int:
    """Counts an iterable matching a selection"""
    itr = return_iterable(conn, sel)
    n_sel = 0
    for _ in itr:
        n_sel += 1
    return n_sel


def print_select(conn, stream: TextIO, sel) -> None:
    """Prints all the rows matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    for row in sel_result:
        stream.write(f"{str(row)}\n")


def get_count_query(level: LevelEnum, db_id: Optional[DbId]):
    """Return the query to count rows matching an id"""
    count_key = get_parent_field(level)
    if count_key is None:
        count_key = get_primary_key(level)
        return func.count(count_key)
    if db_id is not None:
        return func.count(count_key == db_id[level])
    return func.count(count_key)


def get_row_query(level: LevelEnum, db_id: DbId, columns=None):
    """Returns the selection a single row given db_id"""
    table = get_table(level)
    prim_key = get_primary_key(level)
    if columns is None:
        sel = select(table).where(prim_key == db_id[level])
    else:
        sel = select(columns).where(prim_key == db_id[level])
    return sel


def get_rows_with_status_query(level: LevelEnum, status: StatusEnum):
    """Returns the selection for all rows with a particular status"""
    prim_key = get_primary_key(level)
    status_key = get_status_key(level)
    sel = select([prim_key]).where(status_key == status)
    return sel


def get_id_match_query(level: LevelEnum, parent_id: Optional[int], match_name: Any):
    """Returns the selection to match a particular ID"""
    prim_key = get_primary_key(level)
    name_field = get_name_field(level)
    parent_field = get_parent_field(level)
    if parent_field is None:
        sel = select([prim_key]).where(name_field == match_name)
    else:
        sel = select([prim_key]).where(and_(parent_field == parent_id, name_field == match_name))
    return sel


def get_match_query(level: LevelEnum, db_id: DbId):
    """Returns the selection all rows given db_id at a given level"""
    table = get_table(level)
    if db_id is None:
        id_tuple = ()
    else:
        id_tuple = db_id.to_tuple()[0 : level.value + 1]
    parent_key = None
    row_id = None
    for i, row_id_ in enumerate(id_tuple):
        if row_id_ is not None:
            parent_key = get_matching_key(level, LevelEnum(i))
            row_id = row_id_
    if parent_key is None:
        sel = select(table)
    else:
        sel = select(table).where(parent_key == row_id)
    return sel


def add_prerequisite(conn, depend_id: DbId, prereq_id: DbId):
    """Inserts a dependency"""
    insert_vals = dict(
        p_id=prereq_id[LevelEnum.production],
        c_id=prereq_id[LevelEnum.campaign],
        s_id=prereq_id[LevelEnum.step],
        g_id=prereq_id[LevelEnum.group],
        w_id=prereq_id[LevelEnum.workflow],
        depend_p_id=depend_id[LevelEnum.production],
        depend_c_id=depend_id[LevelEnum.campaign],
        depend_s_id=depend_id[LevelEnum.step],
        depend_g_id=depend_id[LevelEnum.group],
        depend_w_id=depend_id[LevelEnum.workflow],
    )
    conn.add(Dependency(**insert_vals))
    conn.commit()


def get_prerequisites(conn, level: LevelEnum, db_id: DbId):
    depend_key = get_depend_key(level)
    sel = select(Dependency).where(depend_key == db_id[level])
    itr = return_iterable(conn, sel)
    db_id_list = [DbId.create_from_row(row_) for row_ in itr]
    return db_id_list


def add_script(conn, **kwargs) -> int:
    """Insert a new row with details about a script"""
    conn.add(Script(**kwargs))
    conn.commit()
    counter = func.count(Script.x_id)
    return return_count(conn, counter)


def get_script(conn, script_id: int):
    sel = select(Script).where(Script.x_id == script_id)
    return return_single_row(conn, sel)[0]


def insert_values(conn, level: LevelEnum, **kwargs):
    """Inserts a new row at a given level with values given in kwargs"""
    table = get_table(level)
    conn.add(table(**kwargs))
    conn.commit()


def update_values(conn, level: LevelEnum, db_id: DbId, **kwargs):
    """Updates a given row with values given in kwargs"""
    table = get_table(level)
    prim_key = get_primary_key(level)
    stmt = update(table).where(prim_key == db_id[level]).values(**kwargs)
    upd_result = conn.execute(stmt)
    _check_result(upd_result)


def update_script_status(conn, script_id: int, script_status: StatusEnum) -> None:

    stmt = update(Script).where(Script.x_id == script_id).values(status=script_status)
    upd_result = conn.execute(stmt)
    _check_result(upd_result)
