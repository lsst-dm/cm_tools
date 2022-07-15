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
    MetaData,
    String,
    Table,
    and_,
    create_engine,
    func,
    select,
)

script_meta = MetaData()
script_table = Table(
    "script",
    script_meta,
    Column("x_id", Integer, primary_key=True),  # Unique script ID
    Column("script_url", String),  # Url for script
    Column("log_url", String),  # Url for log
    Column("config_url", String),  # Url for config
    Column("checker", String),  # Checker class
    Column("status", Enum(StatusEnum)),  # Status flag
)

production_meta = MetaData()
production_table = Table(
    "production",
    production_meta,
    Column("p_id", Integer, primary_key=True),  # Unique production ID
    Column("p_name", String, unique=True),  # Production Name
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_child", Integer, default=0),  # Number of associated children
)

campaign_meta = MetaData()
campaign_table = Table(
    "campaign",
    campaign_meta,
    Column("c_id", Integer, primary_key=True),  # Unique campaign ID
    Column("p_id", Integer, ForeignKey(production_table.c.p_id)),
    Column("fullname", String, unique=True),  # Full name of this campaign
    Column("c_name", String),  # Campaign Name
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("butler_repo", String),  # URL for butler repository
    Column("prod_base_url", String),  # URL for root of the production area
    Column("n_child", Integer, default=0),  # Number of associated children
    Column("prepare_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("collect_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("data_query", String),  # Data query
    Column("coll_source", String),  # Source data collection
    Column("coll_in", String),  # Input data collection (post-query)
    Column("coll_out", String),  # Output data collection
    Column("status", Enum(StatusEnum)),  # Status flag
)

step_meta = MetaData()
step_table = Table(
    "step",
    step_meta,
    Column("s_id", Integer, primary_key=True),  # Unique Step ID
    Column("p_id", Integer, ForeignKey(production_table.c.p_id)),
    Column("c_id", Integer, ForeignKey(campaign_table.c.c_id)),
    Column("fullname", String, unique=True),  # Full name of this step
    Column("s_name", String),  # Step Name
    Column("previous_step_id", Integer),  # Unique ID of pervious step
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_child", Integer, default=0),  # Number of associated children
    Column("prepare_script_url", String),  # Script run to prepare data
    Column("prepare_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("collect_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("data_query", String),  # Data query
    Column("coll_source", String),  # Source data collection
    Column("coll_in", String),  # Input data collection (post-query)
    Column("coll_out", String),  # Output data collection
    Column("status", Enum(StatusEnum)),  # Status flag
)

group_meta = MetaData()
group_table = Table(
    "group",
    group_meta,
    Column("g_id", Integer, primary_key=True),  # Unique Group ID
    Column("p_id", Integer, ForeignKey(production_table.c.p_id)),
    Column("c_id", Integer, ForeignKey(campaign_table.c.c_id)),
    Column("s_id", Integer, ForeignKey(step_table.c.s_id)),
    Column("fullname", String, unique=True),  # Full name of this group
    Column("g_name", String),  # Group Name
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_workflows", Integer, default=0),  # Number of associated workflows
    Column("n_child", Integer, default=0),  # Number of associated children
    Column("prepare_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("collect_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("data_query", String),  # Data query
    Column("coll_source", String),  # Source data collection
    Column("coll_in", String),  # Input data collection (post-query)
    Column("coll_out", String),  # Output data collection
    Column("status", Enum(StatusEnum)),  # Status flag
)

workflow_meta = MetaData()
workflow_table = Table(
    "workflow",
    workflow_meta,
    Column("w_id", Integer, primary_key=True),  # Unique Workflow ID
    Column("p_id", Integer, ForeignKey(production_table.c.p_id)),
    Column("c_id", Integer, ForeignKey(campaign_table.c.c_id)),
    Column("s_id", Integer, ForeignKey(step_table.c.s_id)),
    Column("g_id", Integer, ForeignKey(group_table.c.g_id)),
    Column("w_idx", Integer),  # Index for this workflow
    Column("fullname", String, unique=True),  # Full name of this workflow
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_tasks_all", Integer, default=0),  # Number of associated tasks
    Column("n_tasks_done", Integer, default=0),  # Number of finished tasks
    Column("n_tasks_failed", Integer, default=0),  # Number of failed tasks
    Column("n_clusters_all", Integer, default=0),  # Number of associated clusters
    Column("n_clusters_done", Integer, default=0),  # Number of finished clusters
    Column("n_clusters_failed", Integer, default=0),  # Number of failed clusters
    Column("workflow_start", DateTime),  # Workflow start time
    Column("workflow_end", DateTime),  # Workflow end time
    Column("workflow_cputime", Float),
    Column("workflow_tmpl_url", String),  # URL template for workflow yaml
    Column("workflow_subm_url", String),  # URL for as submitted workflow yaml
    Column("prepare_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("collect_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("run_script", Integer, ForeignKey(script_table.c.x_id)),
    Column("data_query", String),  # Data query
    Column("coll_source", String),  # Source data collection
    Column("coll_in", String),  # Input data collection (post-query)
    Column("coll_out", String),  # Output data collection
    Column("status", Enum(StatusEnum)),  # Status flag
)

dependency_meta = MetaData()
dependency_table = Table(
    "dependency",
    dependency_meta,
    Column("d_id", Integer, primary_key=True),  # Unique dependency ID
    Column("p_id", Integer, ForeignKey(production_table.c.p_id)),
    Column("c_id", Integer, ForeignKey(campaign_table.c.c_id)),
    Column("s_id", Integer, ForeignKey(step_table.c.s_id)),
    Column("g_id", Integer, ForeignKey(group_table.c.g_id)),
    Column("w_id", Integer, ForeignKey(workflow_table.c.w_id)),
    Column("depend_p_id", Integer, ForeignKey(production_table.c.p_id)),
    Column("depend_c_id", Integer, ForeignKey(campaign_table.c.c_id)),
    Column("depend_s_id", Integer, ForeignKey(step_table.c.s_id)),
    Column("depend_g_id", Integer, ForeignKey(group_table.c.g_id)),
    Column("depend_w_id", Integer, ForeignKey(workflow_table.c.w_id)),
)


def create_db(engine) -> None:
    """Creates a database as specific by `engine.url`

    Populates the database with empty tables
    """
    from sqlalchemy_utils import create_database  # pylint: disable=import-outside-toplevel

    create_database(engine.url)
    for meta in [
        production_meta,
        campaign_meta,
        step_meta,
        group_meta,
        workflow_meta,
        dependency_meta,
        script_meta,
    ]:
        meta.create_all(engine)


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
        LevelEnum.production: production_table,
        LevelEnum.campaign: campaign_table,
        LevelEnum.step: step_table,
        LevelEnum.group: group_table,
        LevelEnum.workflow: workflow_table,
    }
    return all_tables[level]


def get_primary_key(level: LevelEnum) -> Column:
    """Return the primary key in the table corresponding to a `level`"""
    all_keys = {
        LevelEnum.production: production_table.c.p_id,
        LevelEnum.campaign: campaign_table.c.c_id,
        LevelEnum.step: step_table.c.s_id,
        LevelEnum.group: group_table.c.g_id,
        LevelEnum.workflow: workflow_table.c.w_id,
    }
    return all_keys[level]


def get_status_key(level: LevelEnum) -> Optional[Column]:
    """Return the primary key in the table corresponding to a `level`"""
    all_keys = {
        LevelEnum.production: None,
        LevelEnum.campaign: campaign_table.c.status,
        LevelEnum.step: step_table.c.status,
        LevelEnum.group: group_table.c.status,
        LevelEnum.workflow: workflow_table.c.status,
    }
    return all_keys[level]


def get_name_field(level: LevelEnum) -> Column:
    """Return the `name` field in a table corresponding to a `level`"""
    all_keys = {
        LevelEnum.production: production_table.c.p_name,
        LevelEnum.campaign: campaign_table.c.c_name,
        LevelEnum.step: step_table.c.s_name,
        LevelEnum.group: group_table.c.g_name,
        LevelEnum.workflow: workflow_table.c.w_idx,
    }
    return all_keys[level]


def get_parent_field(level: LevelEnum) -> Optional[Column]:
    """Return the id field of the parent entry in a table
    corresponding to a `level`
    """
    all_keys = {
        LevelEnum.production: None,
        LevelEnum.campaign: campaign_table.c.p_id,
        LevelEnum.step: step_table.c.c_id,
        LevelEnum.group: group_table.c.s_id,
        LevelEnum.workflow: workflow_table.c.g_id,
    }
    return all_keys[level]


def get_n_child_field(level: Optional[LevelEnum]) -> Optional[Column]:
    """Return the id field for the number of childern
    corresponding to an entry at `level`
    """
    all_keys = {
        LevelEnum.production: production_table.c.n_child,
        LevelEnum.campaign: campaign_table.c.n_child,
        LevelEnum.step: step_table.c.n_child,
        LevelEnum.group: group_table.c.n_child,
        LevelEnum.workflow: None,
    }
    return all_keys[level]


def get_matching_key(table_level: LevelEnum, match_level: LevelEnum) -> Column:
    """Return the id field that can be used to match all
    entries in a particular table with any level of parent
    """
    all_keys = {
        LevelEnum.production: [production_table.c.p_id],
        LevelEnum.campaign: [campaign_table.c.p_id, campaign_table.c.c_id],
        LevelEnum.step: [step_table.c.p_id, step_table.c.c_id, step_table.c.s_id],
        LevelEnum.group: [group_table.c.p_id, group_table.c.c_id, group_table.c.s_id, group_table.c.g_id],
        LevelEnum.workflow: [
            workflow_table.c.p_id,
            workflow_table.c.c_id,
            workflow_table.c.s_id,
            workflow_table.c.g_id,
            workflow_table.c.w_id,
        ],
    }
    return all_keys[table_level][match_level.value]


def get_depend_key(level: LevelEnum):
    """Return the id field of the dependency entry
    corresponding to a `level`
    """
    all_keys = {
        LevelEnum.production: dependency_table.c.depend_p_id,
        LevelEnum.campaign: dependency_table.c.depend_c_id,
        LevelEnum.step: dependency_table.c.depend_s_id,
        LevelEnum.group: dependency_table.c.depend_g_id,
        LevelEnum.workflow: dependency_table.c.depend_w_id,
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
            "workflow_tmpl_url",
            "workflow_subm_url",
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
    return sel_result


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


def get_repo_coll():
    """Return the column that has the butler repo"""
    return campaign_table.c.butler_repo


def get_prod_base_coll():
    """Return the column that has the production base area"""
    return campaign_table.c.prod_base_url


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
        sel = table.select().where(prim_key == db_id[level])
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
        sel = table.select()
    else:
        sel = table.select().where(parent_key == row_id)
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
    ins = dependency_table.insert().values(**insert_vals)
    ins_result = conn.execute(ins)
    _check_result(ins_result)


def get_prerequisites(conn, level: LevelEnum, db_id: DbId):
    depend_key = get_depend_key(level)
    sel = dependency_table.select().where(depend_key == db_id[level])
    itr = return_iterable(conn, sel)
    db_id_list = [DbId.create_from_row(row_) for row_ in itr]
    return db_id_list


def add_script(conn, **kwargs) -> int:
    """Insert a new row with details about a script"""
    ins = script_table.insert().values(**kwargs)
    ins_result = conn.execute(ins)
    _check_result(ins_result)
    counter = func.count(script_table.c.x_id)
    return return_count(conn, counter)


def get_script(conn, script_id: int):
    sel = script_table.select().where(script_table.c.x_id == script_id)
    return return_single_row(conn, sel)


def insert_values(conn, level: LevelEnum, **kwargs):
    """Inserts a new row at a given level with values given in kwargs"""
    table = get_table(level)
    ins = table.insert().values(**kwargs)
    ins_result = conn.execute(ins)
    _check_result(ins_result)


def update_values(conn, level: LevelEnum, db_id: DbId, **kwargs):
    """Updates a given row with values given in kwargs"""
    table = get_table(level)
    prim_key = get_primary_key(level)
    stmt = table.update().where(prim_key == db_id[level]).values(**kwargs)
    upd_result = conn.execute(stmt)
    _check_result(upd_result)


def update_script_status(conn, script_id: int, script_status: StatusEnum) -> None:

    stmt = script_table.update().where(script_table.c.x_id == script_id).values(status=script_status)
    upd_result = conn.execute(stmt)
    _check_result(upd_result)
