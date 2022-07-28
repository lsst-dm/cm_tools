import os
import sys
from time import sleep
from typing import Any, Optional, TextIO

from lsst.cm.tools.core.db_interface import CMTableBase, DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db import common, top
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.workflow import Workflow
from sqlalchemy import select
from sqlalchemy.orm import Session


class SQLAlchemyInterface(DbInterface):
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
        sel = table.get_row_query(db_id)
        return common.return_first_column(self, sel)

    def print_(self, stream: TextIO, level: LevelEnum, db_id: DbId) -> None:
        table = top.get_table_for_level(level)
        sel = table.get_match_query(db_id)
        common.print_select(self, stream, sel)

    def print_table(self, stream: TextIO, which_table: TableEnum) -> None:
        table = top.get_table(which_table)
        sel = select(table)
        common.print_select(self, stream, sel)

    def print_tree(self, stream: TextIO, level: LevelEnum, db_id: DbId) -> None:
        entry = self.get_entry(level, db_id)
        entry.print_tree(stream)

    def check(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.check(self, entry)
        return db_id_list

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
        if db_id_list:
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
        if db_id_list:
            self.check(level, db_id)
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

    def rollback(self, level: LevelEnum, db_id: DbId, to_status: StatusEnum) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.rollback(self, entry, to_status)
        return db_id_list

    def fake_run(
        self, level: LevelEnum, db_id: DbId, status: StatusEnum = StatusEnum.completed
    ) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        db_id_list: list[DbId] = []
        for workflow_ in entry.w_:
            old_status = entry.status
            if old_status not in [StatusEnum.prepared, StatusEnum.pending, StatusEnum.running]:
                continue
            handler = workflow_.get_handler()
            handler.fake_run_hook(self, workflow_, status)
            db_id_list.append(workflow_.g_.db_id)
        if db_id_list:
            self.check(level, db_id)
        return db_id_list

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
