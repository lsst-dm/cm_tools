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

import os
import sys
from collections.abc import Iterable
from time import sleep
from typing import Any, Optional, TextIO

import numpy as np
from lsst.cm.tools.core.db_interface import CMTableBase, DbInterface, DependencyBase, ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum

# from lsst.cm.tools.db import db
from lsst.cm.tools.db import common, top
from lsst.cm.tools.db.dependency import Dependency
from lsst.cm.tools.db.script import Script
from sqlalchemy.orm import Session


class SQLAlchemyInterface(DbInterface):
    @staticmethod
    def _copy_fields(fields: list[str], **kwargs) -> dict[str, Any]:
        ret_dict = {}
        for field_ in fields:
            if field_ in kwargs:
                ret_dict[field_] = kwargs.get(field_)
        return ret_dict

    def __init__(self, db_url: str, **kwargs):
        self._engine = top.build_engine(db_url, **kwargs)
        self._conn = Session(self._engine, future=True)
        DbInterface.__init__(self)

    def connection(self):
        return self._conn

    def get_prod_base(self, db_id: DbId) -> str:
        table = top.get_table(LevelEnum.campaign)
        sel = table.get_row_query(db_id, [table.prod_base_url])
        return common.return_first_column(self, sel)

    def get_db_id(self, level: LevelEnum, **kwargs) -> DbId:
        if level is None:
            return DbId()
        p_id = self._get_id(LevelEnum.production, None, kwargs.get("production_name"))
        if level == LevelEnum.production:
            return DbId(p_id=p_id)
        c_id = self._get_id(LevelEnum.campaign, p_id, kwargs.get("campaign_name"))
        if level == LevelEnum.campaign:
            return DbId(p_id=p_id, c_id=c_id)
        s_id = self._get_id(LevelEnum.step, c_id, kwargs.get("step_name"))
        if level == LevelEnum.step:
            return DbId(p_id=p_id, c_id=c_id, s_id=s_id)
        g_id = self._get_id(LevelEnum.group, s_id, kwargs.get("group_name"))
        if level == LevelEnum.group:
            return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id)
        w_id = self._get_id(LevelEnum.workflow, g_id, "%06i" % kwargs.get("workflow_idx"))
        return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id, w_id=w_id)

    def get_status(self, level: LevelEnum, db_id: DbId) -> StatusEnum:
        table = top.get_table(level)
        sel = table.get_row_query(db_id, [table.status])
        return common.return_first_column(self, sel)

    def get_prerequisites(self, db_id: DbId) -> list[DbId]:
        return Dependency.get_prerequisites(self, db_id)

    def get_script(self, script_id: int) -> ScriptBase:
        return Script.get_script(self, script_id)

    def print_(self, stream, level: LevelEnum, db_id: DbId) -> None:
        sel = top.get_match_query(level, db_id)
        common.print_select(self, stream, sel)

    def print_table(self, stream: TextIO, level: LevelEnum) -> None:
        sel = top.get_match_query(level, None)
        common.print_select(self, stream, sel)

    def count(self, level: LevelEnum, db_id: Optional[DbId]) -> int:
        counter = top.get_count_query(level, db_id)
        return common.return_count(self, counter)

    def update(self, level: LevelEnum, db_id: DbId, **kwargs) -> None:
        table = top.get_table(level)
        update_fields = self._copy_fields(table.update_fields, **kwargs)
        if update_fields:
            table.update_values(self, db_id, **update_fields)

    def check(self, level: LevelEnum, db_id: DbId, recurse: bool = False, counter: int = 2) -> None:
        if recurse:
            child_level = level.child()
            if child_level is not None:
                self.check(child_level, db_id, recurse)
        itr = self.get_iterable(level, db_id)

        for row_ in itr:
            if row_.status is None:
                continue
            current_status = StatusEnum(row_.status.value)
            new_status = current_status
            if current_status in [StatusEnum.waiting, StatusEnum.completed, StatusEnum.accepted]:
                continue
            if current_status == StatusEnum.preparing:
                new_status = self._check_prepare_script(row_)
            elif current_status == StatusEnum.collecting:
                new_status = self._check_collect_script(row_)
            elif current_status == StatusEnum.running:
                if level == LevelEnum.workflow:
                    new_status = self._check_run_script(row_)
                else:
                    new_status = self._check_children(level, row_)
            elif current_status in [StatusEnum.ready, StatusEnum.pending]:
                if level != LevelEnum.workflow:
                    new_status = self._check_children(level, row_)
            if current_status == new_status:
                continue
            if new_status != StatusEnum.collecting:
                self.update(level, row_.db_id, status=new_status)
                continue
            if level == LevelEnum.workflow:
                collect_script = self._collect_workflow(row_)
            else:
                collect_script = self._collect_children(level, row_)
            if collect_script is None:
                new_status = StatusEnum.completed
            else:
                new_status = self._check_collect_script(row_)
            self.update(level, row_.db_id, status=new_status)
        if counter > 1:
            self.check(level, db_id, recurse, counter=counter - 1)

    def get_data(self, level: LevelEnum, db_id: DbId):
        sel = top.get_row_query(level, db_id)
        return common.return_single_row(self, sel)[0]

    def get_iterable(self, level: LevelEnum, db_id: DbId) -> Iterable:
        if level is None:
            return None
        sel = top.get_match_query(level, db_id)
        return common.return_iterable(self, sel)

    def add_prerequisite(self, depend_id: DbId, prereq_id: DbId) -> DependencyBase:
        return Dependency.add_prerequisite(self, depend_id, prereq_id)

    def add_script(self, **kwargs) -> ScriptBase:
        kwargs.setdefault("status", StatusEnum.ready)
        return Script.add_script(self, **kwargs)

    def insert(
        self,
        level: LevelEnum,
        parent_db_id: DbId,
        handler: Handler,
        **kwargs,
    ) -> CMTableBase:
        kwcopy = kwargs.copy()
        if level.value > LevelEnum.campaign.value:
            kwcopy["prod_base_url"] = self.get_prod_base(parent_db_id)
        table = top.get_table(level)
        insert_fields = table.get_insert_fields(handler, parent_db_id, **kwcopy)
        new_entry = table.insert_values(self, **insert_fields)
        table.post_insert(self, handler, new_entry, **kwargs)
        return new_entry

    def prepare(self, level: LevelEnum, db_id: DbId, **kwargs) -> list[DbId]:
        assert level != LevelEnum.production
        itr = self.get_iterable(level, db_id)
        kwcopy = kwargs.copy()
        db_id_list = []

        for row_ in itr:
            status = row_.status
            if status != StatusEnum.waiting:
                continue
            handler = row_.get_handler()
            if not handler.check_prerequistes(self, row_.db_id):
                continue
            kwcopy.update(config_yaml=row_.config_yaml)
            db_id_list += row_.prepare(self, handler, **kwcopy)
        self.check(level, db_id, recurse=True, counter=2)

        child_level = level.child()
        if child_level is not None:
            self.prepare(child_level, db_id, **kwargs)
        return db_id_list

    def queue_workflows(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        itr = self.get_iterable(LevelEnum.workflow, db_id)
        db_id_list = []
        for row_ in itr:
            status = row_.status
            if status != StatusEnum.ready:
                continue
            db_id_list.append(row_.db_id)
            self.update(LevelEnum.workflow, row_.db_id, status=StatusEnum.pending)
        return db_id_list

    def launch_workflows(self, level: LevelEnum, db_id: DbId, max_running: int) -> list[DbId]:
        n_running = self._count_workflows_at_status(StatusEnum.running)
        if n_running >= max_running:
            return
        itr = self.get_iterable(LevelEnum.workflow, db_id)
        db_id_list = []
        for row_ in itr:
            status = row_.status
            if status != StatusEnum.pending:
                continue
            one_id = row_.db_id
            db_id_list.append(one_id)
            row_.launch(self)
            n_running += 1
            if n_running >= max_running:
                break
        self.check(LevelEnum.workflow, db_id)
        return

    def accept(self, level: LevelEnum, db_id: DbId, recurse: bool = False) -> list[DbId]:
        db_id_list = []
        if recurse:
            recurse_level = LevelEnum.workflow
            while recurse_level.value > level.value:
                db_id_list += self.accept(recurse_level, db_id)
                recurse_level = recurse_level.parent()
        self.check(level, db_id)
        itr = self.get_iterable(level, db_id)
        for row_ in itr:
            status = row_.status
            if status != StatusEnum.completed:
                continue
            db_id_list.append(row_.db_id)
            handler = row_.get_handler()
            itr_child = self.get_iterable(level.child(), db_id)
            handler.accept_hook(level, self, itr_child, row_)
            self.update(level, row_.db_id, status=StatusEnum.accepted)
        return db_id_list

    def reject(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        itr = self.get_iterable(level, db_id)
        db_id_list = []
        for row_ in itr:
            status = row_.status
            if status == StatusEnum.accepted:
                continue
            db_id_list.append(row_.db_id)
            handler = row_.get_handler()
            handler.reject_hook(level, self, row_)
            self.update(level, row_.db_id, status=StatusEnum.rejected)
        return db_id_list

    def fake_run(self, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> None:
        itr = self.get_iterable(LevelEnum.workflow, db_id)
        for row_ in itr:
            old_status = row_.status
            if old_status not in [StatusEnum.running]:
                continue
            handler = row_.get_handler()
            handler.fake_run_hook(self, row_, status)

    def daemon(self, db_id: DbId, max_running: int = 100, sleep_time: int = 60, n_iter: int = -1) -> None:
        i_iter = n_iter
        while i_iter != 0:
            if os.path.exists("daemon.stop"):  # pragma: no cover
                break
            self.prepare(LevelEnum.step, db_id)
            self.queue_workflows(LevelEnum.campaign, db_id)
            self.launch_workflows(LevelEnum.campaign, db_id, max_running)
            self.accept(LevelEnum.campaign, db_id, recurse=True)
            self.print_table(sys.stdout, LevelEnum.step)
            self.print_table(sys.stdout, LevelEnum.group)
            self.print_table(sys.stdout, LevelEnum.workflow)
            i_iter -= 1
            sleep(sleep_time)

    def _count_workflows_at_status(self, status: StatusEnum) -> int:
        sel = top.get_rows_with_status_query(LevelEnum.workflow, status)
        return common.return_select_count(self, sel)

    def _get_id(self, level: LevelEnum, parent_id: Optional[int], match_name: Optional[str]) -> Optional[int]:
        """Returns the primary key matching the parent_id and the match_name"""
        if match_name is None:
            return None
        sel = top.get_id_match_query(level, parent_id, match_name)
        return common.return_first_column(self, sel)

    @staticmethod
    def _extract_child_status(itr: Iterable) -> np.ndarray:
        """Return the status of all children in an array"""
        return np.array([x.status.value for x in itr])

    def _check_children(self, level: LevelEnum, data) -> StatusEnum:
        """Check the status of childern of a given row
        and return a status accordingly"""
        itr = self.get_iterable(level.child(), data.db_id)
        child_status = self._extract_child_status(itr)
        if child_status.size and (child_status >= StatusEnum.accepted.value).all():
            return StatusEnum.collecting
        if (child_status >= StatusEnum.running.value).any():
            return StatusEnum.running
        return data.status

    def _collect_children(self, level: LevelEnum, data) -> ScriptBase:
        """Make the script to collect output from children"""
        handler = data.get_handler()
        itr_child = self.get_iterable(level.child(), data.db_id)
        return handler.collect_script_hook(level, self, itr_child, data)

    def _collect_workflow(self, data) -> ScriptBase:
        """Check the status of one workflow matching a given db_id"""
        handler = data.get_handler()
        return handler.collect_script_hook(LevelEnum.workflow, self, [], data)

    def _check_prepare_script(self, data) -> StatusEnum:
        """Check the status of one entry matching a given db_id"""
        script_id = data.prepare_script
        script_status = self._check_script(script_id)
        status_map = {
            StatusEnum.failed: StatusEnum.failed,
            StatusEnum.ready: StatusEnum.preparing,
            StatusEnum.running: StatusEnum.preparing,
            StatusEnum.completed: StatusEnum.ready,
        }
        return status_map[script_status]

    def _check_collect_script(self, data) -> StatusEnum:
        """Check the status of one entry matching a given db_id"""
        script_id = data.collect_script
        script_status = self._check_script(script_id)
        status_map = {
            StatusEnum.failed: StatusEnum.failed,
            StatusEnum.ready: StatusEnum.collecting,
            StatusEnum.running: StatusEnum.collecting,
            StatusEnum.completed: StatusEnum.completed,
        }
        return status_map[script_status]

    def _check_run_script(self, data) -> StatusEnum:
        """Check the status of one entry matching a given db_id"""
        script_id = data.run_script
        assert script_id is not None
        script_status = self._check_script(script_id)
        status_map = {
            StatusEnum.failed: StatusEnum.failed,
            StatusEnum.ready: StatusEnum.running,
            StatusEnum.running: StatusEnum.running,
            StatusEnum.completed: StatusEnum.collecting,
        }
        return status_map[script_status]

    def _check_script(self, script_id):
        """Check the status of a script"""
        if script_id is None:
            # No script to check, return completed
            return StatusEnum.completed
        script = self.get_script(script_id)
        return script.check_status(self)
