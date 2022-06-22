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

import sys

from collections.abc import Iterable

from sqlalchemy import create_engine, select, and_, func

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.utils import StatusEnum, LevelEnum
from lsst.cm.tools.db.tables import (
    create_db, get_table,
    get_primary_key, get_name_field, get_parent_field)


class SQLAlchemyInterface(DbInterface):

    def __init__(self, db: str, **kwargs):
        from sqlalchemy_utils import database_exists
        kwcopy = kwargs.copy()
        create = kwcopy.pop('create', False)
        self._engine = create_engine(db, **kwcopy)
        if not database_exists(self._engine.url):
            if create:
                create_db(self._engine)
        if not database_exists(self._engine.url):
            raise RuntimeError(f'Failed to access or create database {db}')
        self._conn = self._engine.connect()

    def _check_result(self, result):
        assert result

    def _return_id(self, sel):
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        return sel_result.all()[0][0]

    def _return_single_row(self, sel):
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        return sel_result.all()[0]

    def _return_iterable(self, sel):
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        return sel_result

    def _return_count(self, count):
        count_result = self._conn.execute(count)
        self._check_result(count_result)
        return count_result.scalar()

    def _print_select(self, sel):
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        for row in sel_result:
            print(row)

    def _get_id(self, level: LevelEnum, parent_id: int, match_name):
        prim_key = get_primary_key(level)
        name_field = get_name_field(level)
        parent_field = get_parent_field(level)
        if parent_field is None:
            sel = select([prim_key]).where(name_field == match_name)
        else:
            sel = select([prim_key]).where(and_(parent_field == parent_id,
                                                name_field == match_name))
        return self._return_id(sel)

    def _get_data(self, level: LevelEnum, row_id: int):
        table = get_table(level)
        prim_key = get_primary_key(level)
        sel = table.select().where(prim_key == row_id)
        return self._return_single_row(sel)

    def _get_iterable(self, level: LevelEnum, row_id: int):
        table = get_table(level)
        parent_key = get_parent_field(level)
        if parent_key is None:
            sel = table.select()
        else:
            sel = table.select().where(parent_key == row_id)
        return self._return_iterable(sel)

    def _insert_values(self, level: LevelEnum, **kwargs):
        table = get_table(level)
        ins = table.insert().values(**kwargs)
        ins_result = self._conn.execute(ins)
        self._check_result(ins_result)

    def _update_values(self, level: LevelEnum, row_id: int, **kwargs):
        table = get_table(level)
        prim_key = get_primary_key(level)
        stmt = table.update().where(prim_key == row_id).values(**kwargs)
        upd_result = self._conn.execute(stmt)
        self._check_result(upd_result)

    def _update(self, level: LevelEnum, row_id: int, **kwargs):
        data = self._get_data(level, row_id)
        itr = self._get_iterable(level, row_id)
        handler = self.get_handler(data['handler'])
        if handler is not None:
            update_args = handler.update(level, self, data, itr, **kwargs)
        else:
            update_args = kwargs
        self._update_values(level, row_id, **update_args)

    def _count(self, level: LevelEnum, row_id: int):
        count_key = get_parent_field(level)
        if count_key is None:
            count_key = get_table(level)
            count = func.count(count_key)
        else:
            count = func.count(count_key == row_id)
        return self._return_count(count)

    def _print(self, level: LevelEnum, row_id: int):
        table = get_table(level)
        parent_key = get_parent_field(level)
        if parent_key is None:
            sel = table.select()
        else:
            sel = table.select().where(parent_key == row_id)
        self._print_select(sel)

    def get_production_id(self, production_name: str) -> int:
        return self._get_id(LevelEnum.production, None, production_name)

    def get_campaign_id(self, production_id: int, campaign_name: str) -> int:
        return self._get_id(LevelEnum.campaign, production_id, campaign_name)

    def get_step_id(self, campaign_id: int, step_name: str) -> int:
        return self._get_id(LevelEnum.step, campaign_id, step_name)

    def get_group_id(self, step_id: int, group_name: str) -> int:
        return self._get_id(LevelEnum.group, step_id, group_name)

    def get_workflow_id(self, group_id: int, workflow_idx: int) -> int:
        return self._get_id(LevelEnum.workflow, group_id, workflow_idx)

    def get_production_data(self, production_id: int):
        return self._get_data(LevelEnum.production, production_id)

    def get_campaign_data(self, campaign_id: int):
        return self._get_data(LevelEnum.campaign, campaign_id)

    def get_step_data(self, step_id: int):
        return self._get_data(LevelEnum.step, step_id)

    def get_group_data(self, group_id: int):
        return self._get_data(LevelEnum.group, group_id)

    def get_workflow_data(self, workflow_id: int):
        return self._get_data(LevelEnum.workflow, workflow_id)

    def get_production_iterable(self) -> Iterable:
        return self._get_iterable(LevelEnum.production, None)

    def get_campaign_iterable(self, production_id: int) -> Iterable:
        return self._get_iterable(LevelEnum.campaign, production_id)

    def get_step_iterable(self, campaign_id: int) -> Iterable:
        return self._get_iterable(LevelEnum.step, campaign_id)

    def get_group_iterable(self, step_id: int) -> Iterable:
        return self._get_iterable(LevelEnum.group, step_id)

    def get_workflow_iterable(self, group_id: int) -> Iterable:
        return self._get_iterable(LevelEnum.workflow, group_id)

    def count_productions(self) -> int:
        return self._count(LevelEnum.production, None)

    def count_campaigns(self, production_id: int) -> int:
        return self._count(LevelEnum.campaign, production_id)

    def count_steps(self, campaign_id: int) -> int:
        return self._count(LevelEnum.step, campaign_id)

    def count_groups(self, step_id: int) -> int:
        return self._count(LevelEnum.group, step_id)

    def count_workflows(self, group_id: int) -> int:
        return self._count(LevelEnum.workflow, group_id)

    def print_productions(self):
        self._print(LevelEnum.production, None)

    def print_campaigns(self, production_id: int):
        self._print(LevelEnum.campaign, production_id)

    def print_steps(self, campaign_id: int):
        self._print(LevelEnum.step, campaign_id)

    def print_groups(self, step_id: int):
        self._print(LevelEnum.group, step_id)

    def print_workflows(self, group_id: int):
        self._print(LevelEnum.workflow, group_id)

    def _insert_production(self, **kwargs):
        ins_values = dict(
            n_campaigns=0)
        ins_values.update(**kwargs)
        self._insert_values(LevelEnum.production, **ins_values)

    def _insert_campaign(self, **kwargs):
        ins_values = dict(
            n_steps_all=0,
            n_steps_done=0,
            n_steps_failed=0,
            c_coll_in="",
            c_coll_out="",
            c_status=StatusEnum.waiting)
        ins_values.update(**kwargs)
        p_id = ins_values['p_id']
        n_campaigns = self.count_campaigns(p_id)
        self._insert_values(LevelEnum.campaign, **ins_values)
        self._update_values(LevelEnum.production, p_id, n_campaigns=n_campaigns+1)

    def _insert_step(self, **kwargs):
        ins_values = dict(
            n_groups_all=0,
            n_groups_done=0,
            n_groups_failed=0,
            s_coll_in="",
            s_coll_out="",
            s_status=StatusEnum.waiting)
        ins_values.update(**kwargs)
        c_id = ins_values['c_id']
        n_steps = self.count_steps(c_id)
        self._insert_values(LevelEnum.step, **ins_values)
        self._update_values(LevelEnum.campaign, c_id, n_steps_all=n_steps+1)

    def _insert_group(self, **kwargs):
        ins_values = dict(
            n_workflows=0,
            g_coll_in="",
            g_coll_out="",
            g_status=StatusEnum.waiting)
        ins_values.update(**kwargs)
        s_id = ins_values['s_id']
        n_groups = self.count_groups(s_id)
        self._insert_values(LevelEnum.group, **ins_values)
        self._update_values(LevelEnum.step, s_id, n_groups_all=n_groups+1)

    def _insert_workflow(self, **kwargs):
        ins_values = dict(
            n_tasks_all=0,
            n_tasks_done=0,
            n_tasks_failed=0,
            n_clusters_all=0,
            n_clusters_done=0,
            n_clusters_failed=0,
            workflow_tmpl_url="",
            workflow_submitted_url="",
            data_query_tmpl="",
            data_query_submitted="",
            command_tmpl="",
            command_submitted="",
            panda_log_url="",
            w_coll_in="",
            w_coll_out="",
            w_status=StatusEnum.waiting)
        ins_values.update(**kwargs)
        g_id = ins_values['g_id']
        n_workflows = self.count_workflows(g_id)
        self._insert_values(LevelEnum.workflow, **ins_values)
        self._update_values(LevelEnum.group, g_id, n_workflows=n_workflows+1)

    def _update_production(self, production_id: int, **kwargs):
        self._update(LevelEnum.production, production_id, **kwargs)

    def _update_campaign(self, campaign_id: int, **kwargs):
        self._update(LevelEnum.campaign, campaign_id, **kwargs)

    def _update_step(self, step_id: int, **kwargs):
        self._update(LevelEnum.step, step_id, **kwargs)

    def _update_group(self, group_id: int, **kwargs):
        self._update(LevelEnum.group, group_id, **kwargs)

    def _update_workflow(self, workflow_id: int, **kwargs):
        self._update(LevelEnum.workflow, workflow_id, **kwargs)


if __name__ == '__main__':

    import argparse

    actions = ['create', 'insert', 'update', 'print', 'count']
    parser = argparse.ArgumentParser(prog=sys.argv[0])

    parser.add_argument('--db', type=str, help='Database', default="sqlite:///cm.db")
    parser.add_argument('--action', type=str, help=f"One of {str(actions)}", default=None)
    parser.add_argument('--production_name', type=str, help="Production Name", default=None)
    parser.add_argument('--campaign_name', type=str, help="Campaign Name", default=None)
    parser.add_argument('--step_name', type=str, help="Step Name", default=None)
    parser.add_argument('--group_name', type=str, help="Group Name", default=None)
    parser.add_argument('--workflow', action='store_true',
                        help="Add a workflow to this group", default=False)
    parser.add_argument('--echo', action='store_true', default=False, help="Echo DB commands")
    parser.add_argument('--handler', type=str, help="Callback handler",
                        default='lsst.cm.tools.core.production.ProductionHandler')
    parser.add_argument('--config_yaml', type=str, help="Configuration Yaml", default=None)

    args = parser.parse_args()

    if args.action not in actions:
        raise ValueError(f"action must be one of {str(actions)}")

    iface = SQLAlchemyInterface(args.db, echo=args.echo, create=args.action == 'create')
    kw_args = dict(handler=args.handler, config_yaml=args.config_yaml)

    if args.production_name is None:
        sys.exit(0)
    if args.action == 'create':
        sys.exit(0)

    if args.action == 'insert':
        level, id_args = iface.get_lower_level_and_args(**args.__dict__)
        kw_args.update(**id_args)
        iface.create(level, **kw_args)
    elif args.action == 'count':
        level, id_args = iface.get_upper_level_and_args(**args.__dict__)
        row_id = iface.get_id(**id_args)
        iface.count(level, row_id)
    elif args.action == 'print':
        level, id_args = iface.get_upper_level_and_args(**args.__dict__)
        row_id = iface.get_id(**id_args)
        iface.print_(level, row_id)
    elif args.action == 'update':
        level, id_args = iface.get_lower_level_and_args(**args.__dict__)
        kw_args.update(**id_args)
        iface.update(level, **kw_args)
