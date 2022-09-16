import os
import sys
from time import sleep
from typing import Any, Iterable, Optional, TextIO

import yaml
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from lsst.cm.tools.core.db_interface import CMTableBase, ConfigBase, DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db import common, top
from lsst.cm.tools.db.config import Config, ConfigAssociation, Fragment
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
        workflow_idx = kwargs.get("workflow_idx", 0)
        w_id = self._get_id(LevelEnum.workflow, g_id, f"{workflow_idx:02}")
        return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id, w_id=w_id)

    def get_entry_from_fullname(self, level: LevelEnum, fullname: str) -> DbId:
        table = top.get_table_for_level(level)
        sel = select(table).where(table.fullname == fullname)
        entry = common.return_first_column(self, sel)
        return entry

    def get_entry_from_parent(self, parent_id: DbId, entry_name: str) -> DbId:
        parent_level = parent_id.level()
        child_level = parent_level.child()
        # This should never be called on workflow level objects
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

    def get_config(self, config_name: str) -> ConfigBase:
        sel = select(Config).where(Config.name == config_name)
        return common.return_first_column(self, sel)

    def get_matching(self, level: LevelEnum, entry: Any, status: StatusEnum) -> Iterable:
        table = top.get_table_for_level(level)
        match_key = table.match_keys[entry.level.value]
        sel = select(table).where(
            and_(
                match_key == entry.id,
                table.status == status,
            )
        )
        return self.connection().execute(sel)

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

    def check(self, level: LevelEnum, db_id: DbId) -> StatusEnum:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        status = handler.check(self, entry)
        return status

    def insert(
        self,
        parent_db_id: DbId,
        config_block: str,
        config: ConfigBase | None,
        **kwargs: Any,
    ) -> CMTableBase:

        if parent_db_id is None:
            # This is only called on production level entries
            # which don't use handlers
            assert config is None
            production = Production.insert_values(self, name=kwargs.get("production_name"))
            self.connection().commit()
            return production
        parent = self.get_entry(parent_db_id.level(), parent_db_id)
        if config is None:
            config = parent.config_
        handler = config.get_sub_handler(config_block)
        ret_val = handler.insert(self, parent, config_id=config.id, **kwargs)
        self.connection().commit()
        self.check(ret_val.level, ret_val.db_id)
        return ret_val

    def queue_jobs(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        db_id_list = []
        for job_ in entry.jobs_:
            status = job_.status
            if status != StatusEnum.ready:
                continue
            db_id_list.append(job_.db_id)
            Job.update_values(self, job_.id, status=StatusEnum.prepared)
        self.connection().commit()
        self.check(level, db_id)
        return db_id_list

    def launch_jobs(self, level: LevelEnum, db_id: DbId, max_running: int) -> list[DbId]:
        db_id_list: list[DbId] = []
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        n_running = 0
        # n_running = self._count_jobs_at_status(StatusEnum.running)
        # if n_running >= max_running:
        #    return db_id_list
        for job_ in entry.jobs_:
            status = job_.status
            if status == StatusEnum.running:
                n_running += 1
            if status != StatusEnum.prepared:
                continue
            db_id_list.append(job_.db_id)
            handler = job_.get_handler()
            handler.launch(self, job_)
            n_running += 1
            if n_running >= max_running:
                break
        self.connection().commit()
        self.check(level, db_id)
        return db_id_list

    def accept(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.accept(self, entry)
        self.connection().commit()
        self.check(level, db_id)
        return db_id_list

    def reject(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.reject(self, entry)
        self.connection().commit()
        self.check(level, db_id)
        return db_id_list

    def rollback(self, level: LevelEnum, db_id: DbId, to_status: StatusEnum) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.rollback(self, entry, to_status)
        self.connection().commit()
        return db_id_list

    def supersede(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        db_id_list = handler.supersede(self, entry)
        self.connection().commit()
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
        self.connection().commit()
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
            if old_status not in [StatusEnum.ready, StatusEnum.prepared, StatusEnum.running]:
                continue
            handler = script_.get_handler()
            handler.fake_run_hook(self, script_, status)
            db_id_list.append(script_.id)
        self.check(level, db_id)
        return db_id_list

    def daemon(self, db_id: DbId, max_running: int = 100, sleep_time: int = 60, n_iter: int = -1) -> None:
        i_iter = n_iter
        while i_iter != 0:
            if os.path.exists("daemon.stop"):  # pragma: no cover
                break
            self.queue_jobs(LevelEnum.campaign, db_id)
            self.launch_jobs(LevelEnum.campaign, db_id, max_running)
            self.accept(LevelEnum.campaign, db_id)
            self.print_table(sys.stdout, TableEnum.step)
            self.print_table(sys.stdout, TableEnum.group)
            self.print_table(sys.stdout, TableEnum.workflow)
            i_iter -= 1
            sleep(sleep_time)

    def parse_config(self, config_name: str, config_yaml: str) -> Config:
        with open(config_yaml, "rt", encoding="utf-8") as config_file:
            config_data = yaml.safe_load(config_file)
        conn = self.connection()
        n_frag = conn.query(func.count(Fragment.id)).scalar()
        frag_names = []
        for key, val in config_data.items():
            fullname = f"{config_name}:{key}"
            includes = val.pop("includes", [])
            data = val.copy()
            for include_ in includes:
                data.update(config_data[include_])
            handler = data.pop("class_name", None)
            if handler is None:
                continue
            new_fragment = Fragment(
                id=n_frag,
                name=config_name,
                tag=key,
                fullname=fullname,
                handler=handler,
                data=data,
            )
            frag_names.append(fullname)
            n_frag += 1
            conn.add(new_fragment)
        return self.build_config(config_name, frag_names)

    def build_config(self, config_name: str, fragment_names: list[str]) -> Config:
        conn = self.connection()
        n_config = conn.query(func.count(Config.id)).scalar()
        new_config = Config(
            id=n_config,
            name=config_name,
        )
        conn.add(new_config)
        frag_list = [
            conn.execute(select(Fragment.id).where(Fragment.fullname == frag_name)).scalar()
            for frag_name in fragment_names
        ]
        for frag_id in frag_list:
            new_assoc = ConfigAssociation(
                frag_id=frag_id,
                config_id=new_config.id,
            )
            conn.add(new_assoc)
        conn.commit()

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
