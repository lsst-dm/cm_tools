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
from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import db
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
        self._engine = db.build_engine(db_url, **kwargs)
        self._conn = Session(self._engine, future=True)
        DbInterface.__init__(self)

    def get_repo(self, db_id: DbId) -> str:
        table = db.get_table(LevelEnum.campaign)
        sel = db.get_row_query(LevelEnum.campaign, db_id, [table.butler_repo])
        return db.return_first_column(self._conn, sel)

    def get_prod_base(self, db_id: DbId) -> str:
        table = db.get_table(LevelEnum.campaign)
        sel = db.get_row_query(LevelEnum.campaign, db_id, [table.prod_base_url])
        return db.return_first_column(self._conn, sel)

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
        table = db.get_table(level)
        sel = db.get_row_query(level, db_id, [table.status])
        return db.return_first_column(self._conn, sel)

    def get_prerequisites(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        return db.get_prerequisites(self._conn, level, db_id)

    def get_script(self, script_id: int) -> ScriptBase:
        return db.get_script(self._conn, script_id)

    def print_(self, stream, level: LevelEnum, db_id: DbId) -> None:
        sel = db.get_match_query(level, db_id)
        db.print_select(self._conn, stream, sel)

    def print_table(self, stream: TextIO, level: LevelEnum) -> None:
        sel = db.get_match_query(level, None)
        db.print_select(self._conn, stream, sel)

    def count(self, level: LevelEnum, db_id: Optional[DbId]) -> int:
        counter = db.get_count_query(level, db_id)
        return db.return_count(self._conn, counter)

    def update(self, level: LevelEnum, db_id: DbId, **kwargs) -> None:
        table = db.get_table(level)
        update_fields = self._copy_fields(table.update_fields, **kwargs)
        if update_fields:
            db.update_values(self._conn, level, db_id, **update_fields)

    def check(self, level: LevelEnum, db_id: DbId, recurse: bool = False, counter: int = 2) -> None:
        if recurse:
            child_level = level.child()
            if child_level is not None:
                self.check(child_level, db_id, recurse)
        itr = self.get_iterable(level, db_id)

        for row_ in itr:
            one_id = row_.db_id
            if row_.status is not None:
                current_status = row_.status
                if current_status == StatusEnum.preparing:
                    current_status = self._check_prepare_script(row_)
                elif current_status == StatusEnum.collecting:
                    current_status = self._check_collect_script(row_)
            else:
                current_status = None
            if level == LevelEnum.workflow:
                if current_status == StatusEnum.running:
                    update_fields = self._check_workflow(one_id, row_)
                else:
                    update_fields = dict(status=current_status)
            else:
                if current_status == StatusEnum.completed:
                    update_fields = dict(status=current_status)
                else:
                    update_fields = self._check_children(level, one_id, current_status, row_)
            if row_.status is not None:
                self.update(level, one_id, **update_fields)
        if counter > 1:
            self.check(level, db_id, recurse, counter=counter - 1)

    def get_data(self, level: LevelEnum, db_id: DbId):
        sel = db.get_row_query(level, db_id)
        return db.return_single_row(self._conn, sel)[0]

    def get_iterable(self, level: LevelEnum, db_id: DbId) -> Iterable:
        if level is None:
            return None
        sel = db.get_match_query(level, db_id)
        return db.return_iterable(self._conn, sel)

    def add_prerequisite(self, depend_id: DbId, prereq_id: DbId) -> None:
        db.add_prerequisite(self._conn, depend_id, prereq_id)

    def add_script(self, **kwargs) -> int:
        kwargs.setdefault("status", StatusEnum.ready)
        return db.add_script(self._conn, **kwargs)

    def insert(
        self,
        level: LevelEnum,
        parent_db_id: DbId,
        handler: Handler,
        **kwargs,
    ) -> dict[str, Any]:
        kwcopy = kwargs.copy()
        if level.value > LevelEnum.campaign.value:
            kwcopy["prod_base_url"] = self.get_prod_base(parent_db_id)
        table = db.get_table(level)
        insert_fields = table.get_insert_fields(handler, parent_db_id, **kwcopy)
        db.insert_values(self._conn, level, **insert_fields)
        insert_fields["id"] = self._current_id(level)
        table.post_insert(self, handler, insert_fields, **kwargs)
        return insert_fields

    def prepare(self, level: LevelEnum, db_id: DbId, **kwargs) -> list[DbId]:
        itr = self.get_iterable(level, db_id)
        kwcopy = kwargs.copy()
        kwcopy["prod_base_url"] = self.get_prod_base(db_id)
        db_id_list = []
        for row_ in itr:
            status = row_.status
            if status != StatusEnum.waiting:
                continue
            one_id = db_id.extend(level, row_.id)
            handler = row_.get_handler()
            kwcopy.update(config_yaml=row_.config_yaml)
            db_id_list += handler.prepare_hook(level, self, one_id, row_, **kwcopy)
        self.check(level, db_id, recurse=True, counter=2)
        return db_id_list

    def queue_workflows(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        itr = self.get_iterable(LevelEnum.workflow, db_id)
        db_id_list = []
        for row_ in itr:
            status = row_.status
            if status != StatusEnum.ready:
                continue
            one_id = row_.db_id
            db_id_list.append(one_id)
            self.update(LevelEnum.workflow, one_id, status=StatusEnum.pending)
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
            handler = row_.get_handler()
            handler.launch_workflow_hook(self, one_id, row_)
            self.update(LevelEnum.workflow, one_id, status=StatusEnum.running)
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
            one_id = row_.db_id
            db_id_list.append(one_id)
            handler = row_.get_handler()
            itr = self.get_iterable(level.child(), db_id)
            handler.accept_hook(level, self, one_id, itr, row_)
            self.update(level, one_id, status=StatusEnum.accepted)
        return db_id_list

    def reject(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        itr = self.get_iterable(level, db_id)
        db_id_list = []
        for row_ in itr:
            status = row_.status
            if status == StatusEnum.accepted:
                continue
            one_id = row_.db_id
            db_id_list.append(one_id)
            handler = row_.get_handler()
            handler.reject_hook(level, self, one_id, row_)
            self.update(level, one_id, status=StatusEnum.rejected)
        return db_id_list

    def fake_run(self, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> None:
        itr = self.get_iterable(LevelEnum.workflow, db_id)
        for row_ in itr:
            old_status = row_.status
            if old_status not in [StatusEnum.running]:
                continue
            one_id = row_.db_id
            handler = row_.get_handler()
            handler.fake_run_hook(self, one_id, row_, status)

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
        sel = db.get_rows_with_status_query(LevelEnum.workflow, status)
        return db.return_select_count(self._conn, sel)

    def _get_id(self, level: LevelEnum, parent_id: Optional[int], match_name: Optional[str]) -> Optional[int]:
        """Returns the primary key matching the parent_id and the match_name"""
        if match_name is None:
            return None
        sel = db.get_id_match_query(level, parent_id, match_name)
        return db.return_first_column(self._conn, sel)

    def _current_id(self, level: LevelEnum) -> int:
        return self.count(level, None)

    @staticmethod
    def _extract_child_status(itr: Iterable) -> np.ndarray:
        """Return the status of all children in an array"""
        return np.array([x.status.value for x in itr])

    def _check_children(
        self, level: LevelEnum, db_id: DbId, current_status: Optional[StatusEnum], data
    ) -> dict[str, Optional[StatusEnum]]:
        """Check the status of childern of a given row
        and return a status accordingly"""
        itr = self.get_iterable(level.child(), db_id)
        child_status = self._extract_child_status(itr)
        if child_status.size and (child_status >= StatusEnum.accepted.value).all():
            handler = data.get_handler()
            itr = self.get_iterable(level.child(), db_id)
            return handler.collection_hook(level, self, db_id, itr, data)
        if (child_status >= StatusEnum.running.value).any():
            return dict(status=StatusEnum.running)
        return dict(status=current_status)

    def _check_workflow(self, db_id: DbId, data):
        """Check the status of one workflow matching a given db_id"""
        run_status = self._check_run_script(data)
        if run_status == StatusEnum.completed:
            handler = data.get_handler()
            return handler.collection_hook(LevelEnum.workflow, self, db_id, [], data)
        return {}

    def _check_prepare_script(self, data) -> StatusEnum:
        """Check the status of one entry matching a given db_id"""
        script_id = data.prepare_script
        script_status = self._check_script(script_id)
        status_map = {
            StatusEnum.failed: StatusEnum.failed,
            StatusEnum.ready: StatusEnum.waiting,
            StatusEnum.running: StatusEnum.waiting,
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
            StatusEnum.completed: StatusEnum.completed,
        }
        return status_map[script_status]

    def _check_script(self, script_id):
        """Check the status of a script"""
        if script_id is None:
            # No script to check, return completed
            return StatusEnum.completed
        script = self.get_script(script_id)
        return script.check_status(self._conn)
