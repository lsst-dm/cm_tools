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
from lsst.cm.tools.core.db_interface import DbId, DbInterface
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.tables import (
    create_db,
    get_matching_key,
    get_n_child_field,
    get_name_field,
    get_parent_field,
    get_primary_key,
    get_prod_base_coll,
    get_repo_coll,
    get_status_key,
    get_table,
)
from sqlalchemy import and_, create_engine, func, select  # type: ignore


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

    def __init__(self, db: str, **kwargs):
        from sqlalchemy_utils import database_exists  # type: ignore

        kwcopy = kwargs.copy()
        create = kwcopy.pop("create", False)
        self._engine = create_engine(db, **kwcopy)
        if not database_exists(self._engine.url):
            if create:
                create_db(self._engine)
        if not database_exists(self._engine.url):
            raise RuntimeError(f"Failed to access or create database {db}")
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
        table = get_table(level)
        prim_key = get_primary_key(level)
        status_key = get_status_key(level)
        sel = table.select().where(prim_key == db_id[level])
        return self._return_single_row(sel)[status_key.name]

    def print_(self, stream, level: LevelEnum, db_id: DbId) -> None:
        sel = self._get_select(level, db_id)
        self._print_select(stream, sel)

    def print_table(self, stream: TextIO, level: LevelEnum) -> None:
        table = get_table(level)
        sel = table.select()
        self._print_select(stream, sel)

    def count(self, level: LevelEnum, db_id: Optional[DbId]):
        count_key = get_parent_field(level)
        if count_key is None:
            count_key = get_primary_key(level)
            counter = func.count(count_key)
        else:
            if db_id is not None:
                counter = func.count(count_key == db_id[level])
            else:
                counter = func.count(count_key)
        return self._return_count(counter)

    def update(self, level: LevelEnum, db_id: DbId, **kwargs) -> None:
        data = self.get_data(level, db_id)
        itr = self.get_iterable(level.child(), db_id)
        handler = Handler.get_handler(data["handler"], data["config_yaml"])
        assert handler is not None
        update_args = handler.get_update_fields_hook(level, self, data, itr, **kwargs)
        if update_args:
            self._update_values(level, db_id, **update_args)

    def check(self, level: LevelEnum, db_id: DbId, recurse: bool = False, counter: int = 2) -> None:
        if recurse:
            child_level = level.child()
            if child_level is not None:
                self.check(child_level, db_id, recurse)
        itr = self.get_iterable(level, db_id)
        status_key = get_status_key(level)

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
        table = get_table(level)
        prim_key = get_primary_key(level)
        sel = table.select().where(prim_key == db_id[level])
        return self._return_single_row(sel)

    def get_iterable(self, level: LevelEnum, db_id: DbId, join_levels: list[LevelEnum] = []) -> Iterable:
        if level is None:
            return None
        sel = self._get_join(level, db_id, join_levels)
        return self._return_iterable(sel)

    def insert(
        self, level: LevelEnum, parent_db_id: DbId, handler: Handler, recurse: bool = True, **kwargs,
    ) -> dict[str, Any]:
        prim_key = get_primary_key(level)
        insert_fields = handler.get_insert_fields_hook(level, self, parent_db_id, **kwargs)
        self._insert_values(level, **insert_fields)
        insert_fields[prim_key.name] = self._current_id(level)
        parent_level = level.parent()
        if parent_level is not None:
            n_siblings_fields = get_n_child_field(parent_level)
            n_siblings = self.count(level, parent_db_id)
            update_fields = {n_siblings_fields.name: n_siblings}
            self._update_values(parent_level, parent_db_id, **update_fields)
        if recurse:
            handler.post_insert_hook(level, self, insert_fields, recurse, **kwargs)
        return insert_fields

    def prepare(self, level: LevelEnum, db_id: DbId, recurse: bool = True, **kwargs) -> list[DbId]:
        itr = self.get_iterable(level, db_id)
        prim_key = get_primary_key(level)
        status_key = get_status_key(level)
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
        status_key = get_status_key(level)
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
        status_key = get_status_key(level)
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
        status_key = get_status_key(LevelEnum.workflow)
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
            if os.path.exists("daemon.stop"):
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

    def _check_result(self, result) -> None:
        """Placeholder function to check on SQL query results"""
        assert result

    def _return_id(self, sel) -> Optional[int]:
        """Returns the first column in the first row matching a selection"""
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        try:
            return sel_result.all()[0][0]
        except IndexError:
            return None

    def _return_single_row(self, sel):
        """Returns the first row matching a selection"""
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        return sel_result.all()[0]

    def _return_iterable(self, sel) -> Iterable:
        """Returns an iterable matching a selection"""
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        return sel_result

    def _return_select_count(self, sel) -> int:
        """Returns an iterable matching a selection"""
        itr = self._return_iterable(sel)
        n_sel = 0
        for _ in itr:
            n_sel += 1
        return n_sel

    def _return_count(self, count) -> int:
        """Returns the number of rows mathcing a selection"""
        count_result = self._conn.execute(count)
        self._check_result(count_result)
        return count_result.scalar()

    def _count_workflows_at_status(self, status: StatusEnum) -> int:
        prim_key = get_primary_key(LevelEnum.workflow)
        status_key = get_status_key(LevelEnum.workflow)
        sel = select([prim_key]).where(status_key == status)
        return self._return_select_count(sel)

    def _print_select(self, stream: TextIO, sel):
        """Prints all the rows matching a selection"""
        sel_result = self._conn.execute(sel)
        self._check_result(sel_result)
        for row in sel_result:
            stream.write(f"{str(row)}\n")

    def _get_id(self, level: LevelEnum, parent_id: Optional[int], match_name: Any) -> Optional[int]:
        """Returns the primary key matching the parent_id and the match_name"""
        if match_name is None:
            return None
        prim_key = get_primary_key(level)
        name_field = get_name_field(level)
        parent_field = get_parent_field(level)
        if parent_field is None:
            sel = select([prim_key]).where(name_field == match_name)
        else:
            sel = select([prim_key]).where(and_(parent_field == parent_id, name_field == match_name))
        return self._return_id(sel)

    def _get_select(self, level: LevelEnum, db_id: DbId):
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

    def _get_join(self, level: LevelEnum, db_id: DbId, join_levels: list[LevelEnum]) -> Iterable:
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

    def _insert_values(self, level: LevelEnum, **kwargs):
        """Inserts a new row at a given level with values given in kwargs"""
        table = get_table(level)
        ins = table.insert().values(**kwargs)
        ins_result = self._conn.execute(ins)
        self._check_result(ins_result)

    def _update_values(self, level: LevelEnum, db_id: DbId, **kwargs):
        """Updates a given row with values given in kwargs"""
        table = get_table(level)
        prim_key = get_primary_key(level)
        stmt = table.update().where(prim_key == db_id[level]).values(**kwargs)
        upd_result = self._conn.execute(stmt)
        self._check_result(upd_result)

    def _current_id(self, level: LevelEnum) -> int:
        return self.count(level, None)

    def get_repo(self, db_id: DbId) -> str:
        repo_col = get_repo_coll()
        prim_key = get_primary_key(LevelEnum.campaign)
        sel = select(repo_col).where(prim_key == db_id[LevelEnum.campaign])
        return self._return_single_row(sel)[0]

    def get_prod_base(self, db_id: DbId) -> str:
        prod_base_coll = get_prod_base_coll()
        prim_key = get_primary_key(LevelEnum.campaign)
        sel = select(prod_base_coll).where(prim_key == db_id[LevelEnum.campaign])
        return self._return_single_row(sel)[0]

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
        new_status = current_status
        if child_status.size and (child_status >= StatusEnum.accepted.value).all():
            handler = Handler.get_handler(data["handler"], data["config_yaml"])
            itr = self.get_iterable(level.child(), db_id)
            new_status = handler.collection_hook(level, self, db_id, itr, data)
        elif (child_status >= StatusEnum.running.value).any():
            new_status = StatusEnum.running
        update_fields: dict[str, Optional[StatusEnum]] = dict(status=new_status)
        return update_fields

    def _check_workflow(self, db_id: DbId, data):
        """Check the status of one workflow matching a given db_id"""
        handler = Handler.get_handler(data["handler"], data["config_yaml"])
        update_fields = handler.check_workflow_status_hook(self, db_id, data)
        if update_fields["status"] == StatusEnum.completed:
            new_status = handler.collection_hook(LevelEnum.workflow, self, db_id, [], data)
            update_fields["status"] = new_status
        return update_fields

    def _check_prepare_script(self, data) -> StatusEnum:
        """Check the status of one entry matching a given db_id"""
        script_url = data["prepare_script_url"]
        log_url = data["prepare_log_url"]
        if script_url is None:
            # No script to run, move to ready
            return StatusEnum.ready
        handler = Handler.get_handler(data["handler"], data["config_yaml"])
        script_status = handler.check_script_status_hook(log_url)
        status_map = {
            StatusEnum.failed: StatusEnum.failed,
            StatusEnum.running: StatusEnum.waiting,
            StatusEnum.completed: StatusEnum.ready,
        }
        return status_map[script_status]

    def _check_collect_script(self, data) -> StatusEnum:
        """Check the status of one entry matching a given db_id"""
        script_url = data["collect_script_url"]
        log_url = data["collect_log_url"]
        if script_url is None:
            # No script to run, move to completed
            return StatusEnum.completed
        handler = Handler.get_handler(data["handler"], data["config_yaml"])
        script_status = handler.check_script_status_hook(log_url)
        status_map = {
            StatusEnum.failed: StatusEnum.failed,
            StatusEnum.running: StatusEnum.collecting,
            StatusEnum.completed: StatusEnum.completed,
        }
        return status_map[script_status]
