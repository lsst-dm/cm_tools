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

from sqlalchemy import MetaData, Table, Column, Float, Integer, String, DateTime, Enum

from lsst.cm.tools.core.utils import StatusEnum, LevelEnum


production_meta = MetaData()
production_table = Table(
    'production', production_meta,
    Column('p_id', Integer, primary_key=True),
    Column('p_name', String),
    Column('handler', String),
    Column('config_yaml', String),
    Column('n_campaigns', Integer)
)

campaign_meta = MetaData()
campaign_table = Table(
    'campaign', campaign_meta,
    Column('c_id', Integer, primary_key=True),
    Column('fullname', String),
    Column('c_name', String),
    Column('p_id', Integer),
    Column('handler', String),
    Column('config_yaml', String),
    Column('n_steps_all', Integer),
    Column('n_steps_done', Integer),
    Column('n_steps_failed', Integer),
    Column('c_coll_in', String),
    Column('c_coll_out', String),
    Column('c_status', Enum(StatusEnum))
)

step_meta = MetaData()
step_table = Table(
    'step', step_meta,
    Column('s_id', Integer, primary_key=True),
    Column('fullname', String),
    Column('s_name', String),
    Column('p_id', Integer),
    Column('c_id', Integer),
    Column('handler', String),
    Column('config_yaml', String),
    Column('n_groups_all', Integer),
    Column('n_groups_done', Integer),
    Column('n_groups_failed', Integer),
    Column('s_coll_in', String),
    Column('s_coll_out', String),
    Column('s_status', Enum(StatusEnum))
)

group_meta = MetaData()
group_table = Table(
    'group', group_meta,
    Column('g_id', Integer, primary_key=True),
    Column('fullname', String),
    Column('g_name', String),
    Column('p_id', Integer),
    Column('c_id', Integer),
    Column('s_id', Integer),
    Column('handler', String),
    Column('config_yaml', String),
    Column('n_workflows', Integer),
    Column('g_coll_in', String),
    Column('g_coll_out', String),
    Column('g_status', Enum(StatusEnum))
)

workflow_meta = MetaData()
workflow_table = Table(
    'workflow', workflow_meta,
    Column('w_id', Integer, primary_key=True),
    Column('fullname', String),
    Column('p_id', Integer),
    Column('c_id', Integer),
    Column('s_id', Integer),
    Column('g_id', Integer),
    Column('w_idx', Integer),
    Column('handler', String),
    Column('config_yaml', String),
    Column('n_tasks_all', Integer),
    Column('n_tasks_done', Integer),
    Column('n_tasks_failed', Integer),
    Column('n_clusters_all', Integer),
    Column('n_clusters_done', Integer),
    Column('n_clusters_failed', Integer),
    Column('workflow_start', DateTime),
    Column('workflow_end', DateTime),
    Column('workflow_cputime', Float),
    Column('workflow_tmpl_url', String),
    Column('workflow_submitted_url', String),
    Column('data_query_tmpl', String),
    Column('data_query_submitted', String),
    Column('command_tmpl', String),
    Column('command_submitted', String),
    Column('panda_log_url', String),
    Column('w_coll_in', String),
    Column('w_coll_out', String),
    Column('w_status', Enum(StatusEnum))
)


def create_db(engine):
    from sqlalchemy_utils import create_database
    create_database(engine.url)
    for meta in [production_meta, campaign_meta, step_meta, group_meta, workflow_meta]:
        meta.create_all(engine)


def get_table(level: LevelEnum):
    all_tables = {
        LevelEnum.production: production_table,
        LevelEnum.campaign: campaign_table,
        LevelEnum.step: step_table,
        LevelEnum.group: group_table,
        LevelEnum.workflow: workflow_table}
    return all_tables[level]


def get_primary_key(level: LevelEnum):
    all_keys = {
        LevelEnum.production: production_table.c.p_id,
        LevelEnum.campaign: campaign_table.c.c_id,
        LevelEnum.step: step_table.c.s_id,
        LevelEnum.group: group_table.c.g_id,
        LevelEnum.workflow: workflow_table.c.w_id}
    return all_keys[level]


def get_name_field(level: LevelEnum):
    all_keys = {
        LevelEnum.production: production_table.c.p_name,
        LevelEnum.campaign: campaign_table.c.c_name,
        LevelEnum.step: step_table.c.s_name,
        LevelEnum.group: group_table.c.g_name,
        LevelEnum.workflow: workflow_table.c.w_idx}
    return all_keys[level]


def get_parent_field(level: LevelEnum):
    all_keys = {
        LevelEnum.production: None,
        LevelEnum.campaign: campaign_table.c.p_id,
        LevelEnum.step: step_table.c.c_id,
        LevelEnum.group: group_table.c.s_id,
        LevelEnum.workflow: workflow_table.c.g_id}
    return all_keys[level]
