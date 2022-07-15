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
from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import db


class SQLAlchemyInterface(DbInterface):

    full_name_templates = [
        "{production_name}",
        "{production_name}/{campaign_name}",
        "{production_name}/{campaign_name}/{step_name}",
        "{production_name}/{campaign_name}/{step_name}/{group_name}",
        "{production_name}/{campaign_name}/{step_name}/{group_name}/{workflow_idx:06}",
    ]

    @classmethod
    def full_name(cls, level: LevelEnum, **kwargs) -> str:
        """Utility function to return a full name
        associated to a database entry"""
        if level is None:
            return None
        return cls.full_name_templates[level.value].format(**kwargs)

    def __init__(self, db_url: str, **kwargs):
        self._engine = db.build_engine(db_url, **kwargs)
        self._conn = self._engine.connect()
        DbInterface.__init__(self)

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
        w_id = self._get_id(LevelEnum.workflow, g_id, kwargs.get("workflow_idx"))
        return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id, w_id=w_id)

    def get_status(self, level: LevelEnum, db_id: DbId) -> StatusEnum:
        status_key = db.get_status_key(level)
        sel = db.get_row_query(level, db_id, [status_key])
        return db.return_first_column(self._conn, sel)

    def get_prerequisites(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        return db.get_prerequisites(self._conn, level, db_id)

    def get_script(self, script_id: int):
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
        data = self.get_data(level, db_id)
        itr = self.get_iterable(level.child(), db_id)
        handler = Handler.get_handler(data["handler"], data["config_yaml"])
        assert handler is not None
        update_args = handler.get_update_fields_hook(level, self, data, itr, **kwargs)
        if update_args:
            db.update_values(self._conn, level, db_id, **update_args)

    def update_script_status(self, script_id: int, script_status: StatusEnum) -> None:
        db.update_script_status(self._conn, script_id, script_status)

    def check(self, level: LevelEnum, db_id: DbId, recurse: bool = False, counter: int = 2) -> None:
        if recurse:
            child_level = level.child()
            if child_level is not None:
                self.check(child_level, db_id, recurse)
        itr = self.get_iterable(level, db_id)
        status_key = db.get_status_key(level)

        for row_ in itr:
            one_id = DbId.create_from_row(row_)
            if status_key is not None:
                current_status = row_[status_key.name]
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
            if status_key is not None:
                self.update(level, one_id, **update_fields)
        if counter > 1:
            self.check(level, db_id, recurse, counter=counter - 1)

    def get_data(self, level: LevelEnum, db_id: DbId):
        sel = db.get_row_query(level, db_id)
        return db.return_single_row(self._conn, sel)

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
        self, level: LevelEnum, parent_db_id: DbId, handler: Handler, recurse: bool = True, **kwargs,
    ) -> dict[str, Any]:
        prim_key = db.get_primary_key(level)
        insert_fields = handler.get_insert_fields_hook(level, self, parent_db_id, **kwargs)
        db.insert_values(self._conn, level, **insert_fields)
        insert_fields[prim_key.name] = self._current_id(level)
        if recurse:
            handler.post_insert_hook(level, self, insert_fields, recurse, **kwargs)
        return insert_fields

    def prepare(self, level: LevelEnum, db_id: DbId, recurse: bool = True, **kwargs) -> list[DbId]:
        itr = self.get_iterable(level, db_id)
        prim_key = db.get_primary_key(level)
        status_key = db.get_status_key(level)
        kwcopy = kwargs.copy()
        db_id_list = []
        for row_ in itr:
            status = row_[status_key.name]
            if status != StatusEnum.waiting:
                continue
            one_id = db_id.extend(level, row_[prim_key.name])
            handler = Handler.get_handler(row_["handler"], row_["config_yaml"])
            kwcopy.update(config_yaml=row_["config_yaml"])
            db_id_list += handler.prepare_hook(level, self, one_id, row_, recurse, **kwcopy)
        self.check(level, db_id, recurse, counter=2)
        return db_id_list

    def queue_workflows(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        itr = self.get_iterable(LevelEnum.workflow, db_id)
        db_id_list = []
        for row_ in itr:
            status = row_["status"]
            if status != StatusEnum.ready:
                continue
            one_id = DbId.create_from_row(row_)
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
            status = row_["status"]
            if status != StatusEnum.pending:
                continue
            one_id = DbId.create_from_row(row_)
            db_id_list.append(one_id)
            handler = Handler.get_handler(row_["handler"], row_["config_yaml"])
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
        status_key = db.get_status_key(level)
        for row_ in itr:
            status = row_[status_key.name]
            if status != StatusEnum.completed:
                continue
            one_id = DbId.create_from_row(row_)
            db_id_list.append(one_id)
            handler = Handler.get_handler(row_["handler"], row_["config_yaml"])
            itr = self.get_iterable(level.child(), db_id)
            handler.accept_hook(level, self, one_id, itr, row_)
            self.update(level, one_id, status=StatusEnum.accepted)
        return db_id_list

    def reject(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        itr = self.get_iterable(level, db_id)
        status_key = db.get_status_key(level)
        db_id_list = []
        for row_ in itr:
            status = row_[status_key.name]
            if status == StatusEnum.completed:
                continue
            one_id = DbId.create_from_row(row_)
            db_id_list.append(one_id)
            handler = Handler.get_handler(row_["handler"], row_["config_yaml"])
            handler.reject_hook(level, self, one_id, row_)
            self.update(level, one_id, status=StatusEnum.rejected)
        return db_id_list

    def fake_run(self, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> None:
        itr = self.get_iterable(LevelEnum.workflow, db_id)
        status_key = db.get_status_key(LevelEnum.workflow)
        for row_ in itr:
            old_status = row_[status_key.name]
            if old_status not in [StatusEnum.running]:
                continue
            one_id = DbId.create_from_row(row_)
            handler = Handler.get_handler(row_["handler"], row_["config_yaml"])
            handler.fake_run_hook(self, one_id, row_, status)

    def daemon(self, db_id: DbId, max_running: int = 100, sleep_time: int = 60, n_iter: int = -1) -> None:
        i_iter = n_iter
        while i_iter != 0:
            if os.path.exists("daemon.stop"):  # pragma: no cover
                break
            self.prepare(LevelEnum.step, db_id, recurse=True)
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

    def _get_id(self, level: LevelEnum, parent_id: Optional[int], match_name: Any) -> Optional[int]:
        """Returns the primary key matching the parent_id and the match_name"""
        if match_name is None:
            return None
        sel = db.get_id_match_query(level, parent_id, match_name)
        return db.return_first_column(self._conn, sel)

    def _current_id(self, level: LevelEnum) -> int:
        return self.count(level, None)

    def get_repo(self, db_id: DbId) -> str:
        repo_col = db.get_repo_coll()
        sel = db.get_row_query(LevelEnum.campaign, db_id, [repo_col])
        return db.return_first_column(self._conn, sel)

    def get_prod_base(self, db_id: DbId) -> str:
        prod_base_coll = db.get_prod_base_coll()
        sel = db.get_row_query(LevelEnum.campaign, db_id, [prod_base_coll])
        return db.return_first_column(self._conn, sel)

    @staticmethod
    def _extract_child_status(itr: Iterable, status_name: str) -> np.ndarray:
        """Return the status of all children in an array"""
        return np.array([x[status_name].value for x in itr])

    def _check_children(
        self, level: LevelEnum, db_id: DbId, current_status: Optional[StatusEnum], data
    ) -> dict[str, Optional[StatusEnum]]:
        """Check the status of childern of a given row
        and return a status accordingly"""
        itr = self.get_iterable(level.child(), db_id)
        child_status = self._extract_child_status(itr, "status")
        if child_status.size and (child_status >= StatusEnum.accepted.value).all():
            handler = Handler.get_handler(data["handler"], data["config_yaml"])
            itr = self.get_iterable(level.child(), db_id)
            return handler.collection_hook(level, self, db_id, itr, data)
        if (child_status >= StatusEnum.running.value).any():
            return dict(status=StatusEnum.running)
        return dict(status=current_status)

    def _check_workflow(self, db_id: DbId, data):
        """Check the status of one workflow matching a given db_id"""
        run_status = self._check_run_script(data)
        if run_status == StatusEnum.completed:
            handler = Handler.get_handler(data["handler"], data["config_yaml"])
            return handler.collection_hook(LevelEnum.workflow, self, db_id, [], data)
        return {}

    def _check_prepare_script(self, data) -> StatusEnum:
        """Check the status of one entry matching a given db_id"""
        script_id = data["prepare_script"]
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
        script_id = data["collect_script"]
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
        script_id = data["run_script"]
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
        script_data = self.get_script(script_id)
        log_url = script_data["log_url"]
        current_status = script_data["status"]
        checker_class = script_data["checker"]
        checker = Checker.get_checker(checker_class)
        script_status = checker.check_url(log_url, current_status)
        if script_status != current_status:
            self.update_script_status(script_id, script_status)
        return script_status
