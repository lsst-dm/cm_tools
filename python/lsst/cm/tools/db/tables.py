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

from typing import Optional

from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Float,
    Integer,  # type: ignore
    MetaData,
    String,
    Table,
)

production_meta = MetaData()
production_table = Table(
    "production",
    production_meta,
    Column("p_id", Integer, primary_key=True),  # Unique production ID
    Column("p_name", String),  # Production Name
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_campaigns", Integer, default=0),  # Number of associated campaigns
)

campaign_meta = MetaData()
campaign_table = Table(
    "campaign",
    campaign_meta,
    Column("c_id", Integer, primary_key=True),  # Unique campaign ID
    Column("fullname", String),  # Full name of this campaign
    Column("c_name", String),  # Campaign Name
    Column("p_id", Integer),  # Parent production ID
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_steps", Integer, default=0),  # Number of associated steps
    Column("n_steps_done", Integer, default=0),  # Number of finished steps
    Column("n_steps_failed", Integer, default=0),  # Number of failed steps
    Column("c_data_query_tmpl", String),  # Template for data query
    Column("c_data_query_subm", String),  # As submitted data query
    Column("c_coll_source", String),  # Source data collection
    Column("c_coll_in", String),  # Input data collection (post-query)
    Column("c_coll_out", String),  # Output data collection
    Column("c_status", Enum(StatusEnum)),  # Campaign status flag
)

step_meta = MetaData()
step_table = Table(
    "step",
    step_meta,
    Column("s_id", Integer, primary_key=True),  # Unique Step ID
    Column("fullname", String),  # Full name of this step
    Column("s_name", String),  # Step Name
    Column("previous_step_id", Integer),  # Unique ID of pervious step
    Column("p_id", Integer),  # Parent production ID
    Column("c_id", Integer),  # Parent campaign ID
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_groups", Integer, default=0),  # Number of associated groups
    Column("n_groups_done", Integer, default=0),  # Number of finished groups
    Column("n_groups_failed", Integer, default=0),  # Number of failed groups
    Column("s_data_query_tmpl", String),  # Template for data query
    Column("s_data_query_subm", String),  # As submitted data query
    Column("s_coll_source", String),  # Source data collection
    Column("s_coll_in", String),  # Input data collection (post-query)
    Column("s_coll_out", String),  # Output data collection
    Column("s_status", Enum(StatusEnum)),  # Step status flag
)

group_meta = MetaData()
group_table = Table(
    "group",
    group_meta,
    Column("g_id", Integer, primary_key=True),  # Unique Group ID
    Column("fullname", String),  # Full name of this group
    Column("g_name", String),  # Group Name
    Column("p_id", Integer),  # Parent production ID
    Column("c_id", Integer),  # Parent campaign ID
    Column("s_id", Integer),  # Parent step ID
    Column("handler", String),  # Handler class
    Column("config_yaml", String),  # Configuration file
    Column("n_workflows", Integer, default=0),  # Number of associated workflows
    Column("g_data_query_tmpl", String),  # Template for data query
    Column("g_data_query_subm", String),  # As submitted data query
    Column("g_coll_source", String),  # Source data collection
    Column("g_coll_in", String),  # Input data collection (post-query)
    Column("g_coll_out", String),  # Output data collection
    Column("g_status", Enum(StatusEnum)),  # Group status flag
)

workflow_meta = MetaData()
workflow_table = Table(
    "workflow",
    workflow_meta,
    Column("w_id", Integer, primary_key=True),  # Unique Workflow ID
    Column("fullname", String),  # Full name of this workflow
    Column("w_idx", Integer),  # Index of this workflow within group
    Column("p_id", Integer),  # Parent production ID
    Column("c_id", Integer),  # Parent campaign ID
    Column("s_id", Integer),  # Parent step ID
    Column("g_id", Integer),  # Parent group ID
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
    Column("command_tmpl", String),  # Template for workflow sumbmission command
    Column("command_subm", String),  # As submitted workflow sumbmission command
    Column("panda_log_url", String),
    Column("w_data_query_tmpl", String),
    Column("w_data_query_subm", String),
    Column("w_coll_source", String),  # Source data collection
    Column("w_coll_in", String),  # Input data collection (post-query)
    Column("w_coll_out", String),  # Output data collection
    Column("w_status", Enum(StatusEnum)),  # Workflow status flag
)


def create_db(engine) -> None:
    """Creates a database as specific by `engine.url`

    Populates the database with empty tables
    """
    from sqlalchemy_utils import (
        create_database,
    )  # pylint: disable=import-outside-toplevel

    create_database(engine.url)
    for meta in [production_meta, campaign_meta, step_meta, group_meta, workflow_meta]:
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
        LevelEnum.campaign: campaign_table.c.c_status,
        LevelEnum.step: step_table.c.s_status,
        LevelEnum.group: group_table.c.g_status,
        LevelEnum.workflow: workflow_table.c.w_status,
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
        LevelEnum.production: production_table.c.n_campaigns,
        LevelEnum.campaign: campaign_table.c.n_steps,
        LevelEnum.step: step_table.c.n_groups,
        LevelEnum.group: group_table.c.n_workflows,
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
        LevelEnum.group: [
            group_table.c.p_id,
            group_table.c.c_id,
            group_table.c.s_id,
            group_table.c.g_id,
        ],
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
    extra_fields: dict[LevelEnum, list[str]] = {
        LevelEnum.production: [],
        LevelEnum.campaign: [
            "n_steps_done",
            "n_steps_failed",
            "c_data_query_tmpl",
            "c_data_query_subm",
            "c_coll_source",
        ],
        LevelEnum.step: [
            "n_groups_done",
            "n_groups_failed",
            "s_data_query_tmpl",
            "s_data_query_subm",
            "s_coll_source",
        ],
        LevelEnum.group: [
            "n_groups_done",
            "n_groups_failed",
            "s_data_query_tmpl",
            "s_data_query_subm",
            "s_coll_source",
        ],
        LevelEnum.workflow: [
            "n_tasks_done",
            "n_tasks_failed",
            "n_clusters_done",
            "n_clusters_failed",
            "workflow_start",
            "workflow_end",
            "workflow_cputime",
            "workflow_tmpl_url",
            "workflow_subm_url",
            "command_tmpl",
            "command_sumb",
            "panda_log_url" "w_data_query_tmpl",
            "w_data_query_subm",
            "w_coll_source",
        ],
    }
    field_list += extra_fields[level]
    return field_list
