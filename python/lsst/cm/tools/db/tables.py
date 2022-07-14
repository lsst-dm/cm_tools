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

from typing import Optional, Iterable, TextIO

from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import select, Column, DateTime, Enum, Float, ForeignKey, MetaData, String, Table


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
    Column("prepare_script_url", String),  # Script run to prepare data
    Column("prepare_log_url", String),  # Url for log from prepare script
    Column("collect_script_url", String),  # Script run to prepare data
    Column("collect_log_url", String),  # Url for log from prepare script
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
    Column("prepare_log_url", String),  # Url for log from prepare script
    Column("collect_script_url", String),  # Script run to prepare data
    Column("collect_log_url", String),  # Url for log from prepare script
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
    Column("prepare_script_url", String),  # Script run to prepare data
    Column("prepare_log_url", String),  # Url for log from prepare script
    Column("collect_script_url", String),  # Script run to prepare data
    Column("collect_log_url", String),  # Url for log from prepare script
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
    Column("prepare_script_url", String),  # Script run to prepare data
    Column("prepare_log_url", String),  # Url for log from prepare script
    Column("run_script_url", String),  # Script to run workflow
    Column("run_log_url", String),  # Url for log from workflow
    Column("collect_script_url", String),  # Script run to prepare data
    Column("collect_log_url", String),  # Url for log from prepare script
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

script_meta = MetaData()
script_table = Table(
    "script",
    script_meta,
    Column("x_id", Integer, primary_key=True),  # Unique script ID
    Column("p_id", Integer, ForeignKey(production_table.c.p_id)),
    Column("c_id", Integer, ForeignKey(campaign_table.c.c_id)),
    Column("s_id", Integer, ForeignKey(step_table.c.s_id)),
    Column("g_id", Integer, ForeignKey(group_table.c.g_id)),
    Column("w_id", Integer, ForeignKey(workflow_table.c.w_id)),
    Column("script_url", String),  # Script run to prepare data
    Column("log_url", String),  # Url for log from prepare script
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
        script_meta
    ]:
        meta.create_all(engine)


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


def get_update_field_list(level: LevelEnum) -> list[str]:
    """Return the list of fields that we can update
    in a particular table
    """
    field_list = ["handler", "config_yaml"]
    common_fields = [
        "prepare_script_url",
        "prepare_log_url",
        "collect_script_url",
        "collect_log_url",
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
        LevelEnum.workflow: common_fields + [
            "n_tasks_done",
            "n_tasks_failed",
            "n_clusters_done",
            "n_clusters_failed",
            "workflow_start",
            "workflow_end",
            "workflow_cputime",
            "workflow_tmpl_url",
            "workflow_subm_url",
            "run_script_url",
            "run_log_url",
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


def return_data(conn, sel):
    """Returns all the data matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    return sel_result.all()


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


def get_select(level: LevelEnum, db_id):
    """Returns the selection for a given db_id at a given level"""
    table = get_table(level)
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


def get_join(level: LevelEnum, db_id, join_levels: list[LevelEnum]) -> Iterable:
    """Returns the joint selection for a given db_id at a given level"""
    table = get_table(level)
    join_tables = [get_table(join_level_) for join_level_ in join_levels]
    join_keys = [get_primary_key(join_level_) for join_level_ in join_levels]
    id_tuple = db_id.to_tuple()[0 : level.value + 1]
    parent_key = None
    row_id = None
    for i, row_id_ in enumerate(id_tuple):
        if row_id_ is not None:
            parent_key = get_matching_key(level, LevelEnum(i))
            row_id = row_id_
    if parent_key is None:
        sel = select(table, *join_tables)
    else:
        sel = select(table, *join_tables).where(parent_key == row_id)
    for join_table, join_key, join_level in zip(join_tables, join_keys, join_levels):
        sel = sel.join(join_table, join_key == id_tuple[join_level.value])
    return sel


def insert_values(conn, level: LevelEnum, **kwargs):
    """Inserts a new row at a given level with values given in kwargs"""
    table = get_table(level)
    ins = table.insert().values(**kwargs)
    ins_result = conn.execute(ins)
    _check_result(ins_result)


def update_values(conn, level: LevelEnum, db_id, **kwargs):
    """Updates a given row with values given in kwargs"""
    table = get_table(level)
    prim_key = get_primary_key(level)
    stmt = table.update().where(prim_key == db_id[level]).values(**kwargs)
    upd_result = conn.execute(stmt)
    _check_result(upd_result)
