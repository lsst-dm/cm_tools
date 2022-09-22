import os
import sys
from time import sleep
from typing import Any, Iterable, Optional, TextIO

import yaml
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from lsst.cm.tools.core.db_interface import CMTableBase, ConfigBase, DbInterface, JobBase, ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
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

    def get_db_id(self, **kwargs: Any) -> DbId:
        fullname = kwargs.get("fullname")
        if fullname is not None:
            return self._get_db_id_from_fullname(fullname)
        return self._get_db_id_in_steps(**kwargs)

    def _get_db_id_in_steps(self, **kwargs: Any) -> DbId:
        p_name = kwargs.get("production_name")
        if p_name is None:
            return DbId()
        p_id = self._get_id(LevelEnum.production, None, p_name)
        c_name = kwargs.get("campaign_name")
        if c_name is None:
            return DbId(p_id=p_id)
        c_id = self._get_id(LevelEnum.campaign, p_id, c_name)
        s_name = kwargs.get("step_name")
        if s_name is None:
            return DbId(p_id=p_id, c_id=c_id)
        s_id = self._get_id(LevelEnum.step, c_id, s_name)
        g_name = kwargs.get("group_name")
        if g_name is None:
            return DbId(p_id=p_id, c_id=c_id, s_id=s_id)
        g_id = self._get_id(LevelEnum.group, s_id, g_name)
        w_idx = kwargs.get("workflow_idx")
        if w_idx is None:
            return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id)
        w_id = self._get_id(LevelEnum.workflow, g_id, f"{w_idx:02}")
        return DbId(p_id=p_id, c_id=c_id, s_id=s_id, g_id=g_id, w_id=w_id)

    @staticmethod
    def parse_fullname(fullname: str) -> dict[str, str]:
        tokens = fullname.split("/")
        n_tokens = len(tokens)
        names = {}
        if n_tokens >= 1:
            names["production_name"] = tokens[0]
        if n_tokens >= 2:
            names["campaign_name"] = tokens[1]
        if n_tokens >= 3:
            names["step_name"] = tokens[2]
        if n_tokens >= 4:
            names["group_name"] = tokens[3]
        if n_tokens >= 5:
            names["workflow_idx"] = tokens[4]
        return names

    def _get_db_id_from_fullname(self, fullname: str) -> DbId:
        names = self.parse_fullname(fullname)
        return self._get_db_id_in_steps(**names)

    def get_entry_from_fullname(self, fullname: str) -> DbId:
        n_slash = fullname.count("/")
        level = LevelEnum(n_slash)
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

    def get_matching(self, level: LevelEnum, entry: CMTableBase, status: StatusEnum) -> Iterable:
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

    def print_config(self, stream: TextIO, config_name: str) -> None:
        config = self.get_config(config_name)
        if config is None:
            raise KeyError(f"No configuration {config_name}")
        stream.write(f"{str(config)}\n")
        for assoc in config.assocs_:
            frag = assoc.frag_
            stream.write(f"  {frag.tag}: {frag.id} {str(frag)}\n")

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
            parent_level = None
        else:
            parent_level = parent_db_id.level()
        if parent_level is None:
            # This is only called on production level entries
            # which don't use handlers
            assert config is None
            production = Production.insert_values(self, name=kwargs.get("production_name"))
            self.connection().commit()
            return production
        parent = self.get_entry(parent_level, parent_db_id)
        if config is None:
            config = parent.config_
        handler = config.get_sub_handler(config_block)
        new_entry = handler.insert(self, parent, config_id=config.id, **kwargs)
        self.connection().commit()
        self.check(new_entry.level, new_entry.db_id)
        return new_entry

    def add_script(
        self,
        parent_db_id: DbId,
        script_name: str,
        config: ConfigBase | None = None,
        **kwargs: Any,
    ) -> ScriptBase:
        parent = self.get_entry(parent_db_id.level(), parent_db_id)
        if config is None:
            config = parent.config_
        handler = config.get_sub_handler(script_name)
        new_script = handler.insert(self, parent, name=script_name, **kwargs)
        self.connection().commit()
        self.check(parent.level, parent.db_id)
        return new_script

    def add_job(
        self,
        parent_db_id: DbId,
        job_name: str,
        config: ConfigBase | None = None,
        **kwargs: Any,
    ) -> JobBase:
        parent = self.get_entry(parent_db_id.level(), parent_db_id)
        if config is None:
            config = parent.config_
        handler = config.get_sub_handler(job_name)
        new_job = handler.insert(self, parent, name=job_name, **kwargs)
        self.connection().commit()
        self.check(parent.level, parent.db_id)
        return new_job

    def queue_jobs(self, level: LevelEnum, db_id: DbId) -> list[DbId]:
        entry = self.get_entry(level, db_id)
        db_id_list = []
        for job_ in entry.jobs_:
            status = job_.status
            if status != StatusEnum.ready:
                continue
            db_id_list.append(job_.db_id)
            handler = job_.get_handler()
            parent = job_.w_
            handler.write_job_hook(self, parent, job_)
            Job.update_values(self, job_.id, status=StatusEnum.prepared)
        self.connection().commit()
        self.check(level, db_id)
        return db_id_list

    def launch_jobs(self, level: LevelEnum, db_id: DbId, max_running: int) -> list[DbId]:
        db_id_list: list[DbId] = []
        entry = self.get_entry(level, db_id)
        handler = entry.get_handler()
        n_running = 0
        # This is what we actually want, but _count_jobs_at_status
        # isn't working correctly under some cases now, so I've
        # commented these lines out until I fix that
        #
        # n_running = self._count_jobs_at_status(StatusEnum.running)
        # if n_running >= max_running:
        #    return db_id_list
        for job_ in entry.jobs_:
            if n_running >= max_running:
                break
            status = job_.status
            if status == StatusEnum.running:
                n_running += 1
            if status != StatusEnum.prepared:
                continue
            db_id_list.append(job_.db_id)
            handler = job_.get_handler()
            handler.launch(self, job_)
            n_running += 1
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

    def supersede_script(self, level: LevelEnum, db_id: DbId, script_name: str) -> list[int]:
        entry = self.get_entry(level, db_id)
        db_id_list: list[int] = []
        for script_ in entry.scripts_:
            if script_.name != script_name:
                continue
            script_.rollback_script(self, entry, script_)
            db_id_list.append(script_.id)
        return db_id_list

    def supersede_job(self, level: LevelEnum, db_id: DbId, job_name: str) -> list[int]:
        entry = self.get_entry(level, db_id)
        db_id_list: list[int] = []
        for job_ in entry.jobs_:
            if job_.name != job_name:
                continue
            job_.rollback_script(self, entry, job_)
            db_id_list.append(job_.id)
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
        frag_names = self._build_fragments(config_name, config_yaml)
        return self._build_config(config_name, frag_names)

    def extend_config(self, config_name: str, config_yaml: str) -> Config:
        conn = self.connection()
        config = conn.execute(select(Config).where(Config.name == config_name)).scalar()
        assert config is not None
        fragment_names = self._build_fragments(config_name, config_yaml)
        frag_list = [
            conn.execute(select(Fragment.id).where(Fragment.fullname == frag_name)).scalar()
            for frag_name in fragment_names
        ]
        for frag_id in frag_list:
            new_assoc = ConfigAssociation(
                frag_id=frag_id,
                config_id=config.id,
            )
            conn.add(new_assoc)
        conn.commit()
        return config

    def _build_fragments(self, config_name: str, config_yaml: str) -> list[str]:
        if Handler.config_dir is not None:
            config_yaml = os.path.join(Handler.config_dir, config_yaml)
        with open(config_yaml, "rt", encoding="utf-8") as config_file:
            config_data = yaml.safe_load(config_file)
        conn = self.connection()
        n_frag = conn.query(func.count(Fragment.id)).scalar()
        frag_names = []
        for key, val in config_data.items():
            fullname = f"{config_name}:{key}"
            if isinstance(val, str):
                assert val[0] == "@"
                fullname = f"{val[1:]}:{key}"
                frag_names.append(fullname)
                continue
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
        return frag_names

    def _build_config(self, config_name: str, fragment_names: list[str]) -> Config:
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
        return new_config

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
