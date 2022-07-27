import os
import sys
from time import sleep
from typing import Any, Optional, TextIO

from lsst.cm.tools.core.db_interface import CMTableBase, DbInterface, DependencyBase, ScriptBase, WorkflowBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db import common, top
from lsst.cm.tools.db.dependency import Dependency
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.script import Script
from lsst.cm.tools.db.workflow import Workflow
from sqlalchemy import select
from sqlalchemy.orm import Session


class SQLAlchemyInterface(DbInterface):
    @staticmethod
    def _copy_fields(fields: list[str], **kwargs: Any) -> dict[str, Any]:
        ret_dict = {}
        for field_ in fields:
            if field_ in kwargs:
                ret_dict[field_] = kwargs.get(field_)
        return ret_dict

    def __init__(self, db_url: str, **kwargs: Any):
        self._engine = top.build_engine(db_url, **kwargs)
        self._conn = Session(self._engine, future=True)
        DbInterface.__init__(self)

    def connection(self) -> Session:
        return self._conn

    def get_db_id(self, level: LevelEnum, **kwargs: Any) -> DbId:
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
        return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id)

    def get_entry(self, level: LevelEnum, db_id: DbId) -> CMTableBase:
        table = top.get_table_for_level(level)
        sel = table.get_row_query(db_id, [])
        return common.return_first_column(self, sel)

    def get_script(self, script_id: int) -> Script:
        return Script.get_script(self, script_id)

    def get_workflow(self, workflow_id: int) -> Workflow:
        return Workflow.get_workflow(self, workflow_id)

    def print_(self, stream: TextIO, which_table: TableEnum, db_id: DbId) -> None:
        table = top.get_table(which_table)
        sel = table.get_match_query(db_id)
        common.print_select(self, stream, sel)

    def print_table(self, stream: TextIO, which_table: TableEnum) -> None:
        table = top.get_table(which_table)
        sel = select(table)
        common.print_select(self, stream, sel)

    def count(self, which_table: TableEnum, db_id: Optional[DbId]) -> int:
        table = top.get_table(which_table)
        counter = table.get_count_query(db_id)
        return common.return_count(self, counter)

    def update(self, level: LevelEnum, row_id: int, **kwargs: Any) -> None:
        table = top.get_table(level)
        update_fields = self._copy_fields(table.update_fields, **kwargs)
        if update_fields:
            table.update_values(self, row_id, **update_fields)

    def check(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.check(self, entry)
        return db_id_list

    def add_prerequisite(self, depend_id: DbId, prereq_id: DbId) -> DependencyBase:
        return Dependency.add_prerequisite(self, depend_id, prereq_id)

    def add_script(self, **kwargs: Any) -> ScriptBase:
        kwargs.setdefault("status", StatusEnum.ready)
        return Script.insert_values(self, **kwargs)

    def add_workflow(self, **kwargs: Any) -> WorkflowBase:
        kwargs.setdefault("status", StatusEnum.ready)
        return Workflow.insert_values(self, **kwargs)

    def insert(
        self,
        parent_db_id: DbId,
        handler: Handler,
        **kwargs: Any,
    ) -> CMTableBase:

        if parent_db_id is None:
            assert handler is None
            production = Production.insert_values(self, name=kwargs.get("production_name"))
            return production
        parent = self.get_entry(handler.level.parent(), parent_db_id)
        return handler.insert(self, parent, **kwargs)

    def prepare(self, level: LevelEnum, db_id: DbId, **kwargs: Any) -> list[DbId]:
        assert level != LevelEnum.production
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.prepare(self, entry)
        self.check(level, db_id)
        return db_id_list

    def queue_workflows(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        db_id_list = []
        for workflow_ in entry.w_:
            status = workflow_.status
            if status != StatusEnum.ready:
                continue
            db_id_list.append(workflow_.db_id)
            Workflow.update_values(self, workflow_.id, status=StatusEnum.pending)
        return db_id_list

    def launch_workflows(self, level: LevelEnum, db_id: DbId, max_running: int) -> list[DbId]:
        db_id_list: list[DbId] = []
        n_running = self._count_workflows_at_status(StatusEnum.running)
        if n_running >= max_running:
            return db_id_list
        entry = self.get_entry(level, db_id)
        for workflow_ in entry.w_:
            status = workflow_.status
            if status != StatusEnum.pending:
                continue
            one_id = workflow_.db_id
            db_id_list.append(one_id)
            handler = workflow_.get_handler()
            handler.launch(self, workflow_)
            n_running += 1
            if n_running >= max_running:
                break
        self.check(level, db_id)
        return db_id_list

    def validate(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.validate(self, entry)
        self.check(level, db_id)
        return db_id_list

    def accept(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.accept(self, entry)
        self.check(level, db_id)
        return db_id_list

    def reject(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.reject(self, entry)
        self.check(level, db_id)
        return db_id_list

    def fake_run(self, level: LevelEnum, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> None:
        entry = self.get_entry(level, db_id)
        for workflow_ in entry.w_:
            old_status = entry.status
            if old_status not in [StatusEnum.pending, StatusEnum.running]:
                continue
            handler = workflow_.get_handler()
            handler.fake_run_hook(self, workflow_, status)
        self.check(level, db_id)

    def daemon(self, db_id: DbId, max_running: int = 100, sleep_time: int = 60, n_iter: int = -1) -> None:
        i_iter = n_iter
        while i_iter != 0:
            if os.path.exists("daemon.stop"):  # pragma: no cover
                break
            self.prepare(LevelEnum.campaign, db_id)
            self.queue_workflows(LevelEnum.campaign, db_id)
            self.launch_workflows(LevelEnum.campaign, db_id, max_running)
            self.accept(LevelEnum.campaign, db_id)
            self.print_table(sys.stdout, TableEnum.step)
            self.print_table(sys.stdout, TableEnum.group)
            self.print_table(sys.stdout, TableEnum.workflow)
            i_iter -= 1
            sleep(sleep_time)

    def _count_workflows_at_status(self, status: StatusEnum) -> int:
        table = top.get_table(TableEnum.workflow)
        sel = table.get_rows_with_status_query(status)
        return common.return_select_count(self, sel)

    def _get_id(self, level: LevelEnum, parent_id: Optional[int], match_name: Optional[str]) -> Optional[int]:
        """Returns the primary key matching the parent_id and the match_name"""
        if match_name is None:
            return None
        table = top.get_table_for_level(level)
        sel = table.get_id_match_query(parent_id, match_name)
        return common.return_first_column(self, sel)
