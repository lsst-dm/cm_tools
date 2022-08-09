import os
import sys
from time import sleep
from typing import Any, Optional, TextIO

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from lsst.cm.tools.core.db_interface import CMTableBase, DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db import common, top
from lsst.cm.tools.db.job import Job
from lsst.cm.tools.db.production import Production


class SQLAlchemyInterface(DbInterface):
    """SQL Alchemy based implemenation of the database interface"""

    def __init__(self, db_url: str, **kwargs: Any):
        self._engine = top.build_engine(db_url, **kwargs)
        self._conn = Session(self._engine, future=True)
        DbInterface.__init__(self)

    def connection(self) -> Session:
        return self._conn

    def get_db_id(self, level: LevelEnum, **kwargs: Any) -> DbId:
        if level is None:
            return DbId()
        fullname = kwargs.get("fullname")
        if fullname:
            entry = self.get_entry_from_fullname(level, fullname)
            return entry.db_id
        return self._get_db_id_in_steps(level, **kwargs)

    def _get_db_id_in_steps(self, level: LevelEnum, **kwargs: Any) -> DbId:
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
        w_id = self._get_id(LevelEnum.workflow, g_id, "%02i" % kwargs.get("workflow_idx", 0))
        return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id, w_id=w_id)

    def get_entry_from_fullname(self, level: LevelEnum, fullname: str) -> DbId:
        table = top.get_table_for_level(level)
        sel = select(table).where(table.fullname == fullname)
        entry = common.return_first_column(self, sel)
        return entry

    def get_entry_from_parent(self, parent_id: DbId, entry_name: str) -> DbId:
        parent_level = parent_id.level()
        child_level = parent_level.child()
        assert child_level
        table = top.get_table_for_level(child_level)
        sel = select(table).where(
            and_(
                table.name == entry_name,
                table.parent_id == parent_id[parent_level],
            )
        )
        entry = common.return_first_column(self, sel)
        return entry

    def get_entry(self, level: LevelEnum, db_id: DbId) -> CMTableBase:
        table = top.get_table_for_level(level)
        sel = select(table).where(table.id == db_id[table.level])
        entry = common.return_first_column(self, sel)
        self._verify_entry(entry, level, db_id)
        return entry

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

    def queue_jobs(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        handler.check(self, entry)
        db_id_list = []
        for job_ in entry.jobs_:
            status = job_.status
            if status != StatusEnum.ready:
                continue
            db_id_list.append(job_.db_id)
            Job.update_values(self, job_.id, status=StatusEnum.prepared)
        if db_id_list:
            self.check(level, db_id)
        return db_id_list

    def launch_jobs(self, level: LevelEnum, db_id: DbId, max_running: int) -> list[DbId]:
        db_id_list: list[DbId] = []
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        handler.check(self, entry)
        n_running = self._count_jobs_at_status(StatusEnum.running)
        if n_running >= max_running:
            return db_id_list
        for job_ in entry.jobs_:
            status = job_.status
            if status != StatusEnum.prepared:
                continue
            db_id_list.append(job_.db_id)
            handler = job_.get_handler()
            handler.launch(self, job_)
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

    def supersede(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.supersede(self, entry)
        return db_id_list

    def fake_run(self, level: LevelEnum, db_id: DbId, status: StatusEnum = StatusEnum.completed) -> list[int]:
        entry = self.get_entry(level, db_id)
        db_id_list: list[int] = []
        for job_ in entry.jobs_:
            old_status = job_.status
            if old_status not in [StatusEnum.prepared, StatusEnum.running]:
                continue
            handler = job_.get_handler()
            handler.fake_run_hook(self, job_, status)
            db_id_list.append(job_.id)
        if db_id_list:
            self.check(level, db_id)
        return db_id_list

    def fake_script(
        self, level: LevelEnum, db_id: DbId, script_name: str, status: StatusEnum = StatusEnum.completed
    ) -> list[int]:
        entry = self.get_entry(level, db_id)
        db_id_list: list[int] = []
        for script_ in entry.scripts_:
            if script_.name != script_name:
                continue
            old_status = script_.status
            if old_status not in [StatusEnum.prepared, StatusEnum.running]:
                continue
            handler = script_.get_handler()
            handler.fake_run_hook(self, script_, status)
            db_id_list.append(script_.id)
        if db_id_list:
            self.check(level, db_id)
        return db_id_list

    def daemon(self, db_id: DbId, max_running: int = 100, sleep_time: int = 60, n_iter: int = -1) -> None:
        i_iter = n_iter
        while i_iter != 0:
            if os.path.exists("daemon.stop"):  # pragma: no cover
                break
            self.prepare(LevelEnum.campaign, db_id)
            self.queue_jobs(LevelEnum.campaign, db_id)
            self.launch_jobs(LevelEnum.campaign, db_id, max_running)
            self.accept(LevelEnum.campaign, db_id)
            self.print_table(sys.stdout, TableEnum.step)
            self.print_table(sys.stdout, TableEnum.group)
            self.print_table(sys.stdout, TableEnum.workflow)
            i_iter -= 1
            sleep(sleep_time)

    def _count_jobs_at_status(self, status: StatusEnum) -> int:
        table = top.get_table(TableEnum.job)
        count = func.count(table.status == status)
        return common.return_count(self, count)

    def _get_id(self, level: LevelEnum, parent_id: Optional[int], match_name: Optional[str]) -> Optional[int]:
        """Returns the primary key matching the parent_id and the match_name"""
        if match_name is None:
            return None
        table = top.get_table_for_level(level)
        parent_field = table.parent_id
        if parent_field is None:
            sel = select([table.id]).where(table.name == match_name)
        else:
            sel = select([table.id]).where(and_(parent_field == parent_id, table.name == match_name))
        return common.return_first_column(self, sel)

    def _verify_entry(self, entry: int | None, level: LevelEnum, db_id: DbId) -> None:
        if entry is None:  # pragma: no cover
            raise ValueError(f"Failed to get entry for {db_id} at {level.name}")
