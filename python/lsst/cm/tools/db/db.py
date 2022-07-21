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

from typing import Any, Iterable, Optional, TextIO

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.core.db_interface import ScriptBase, CMTableBase
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import (  # type: ignore
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    String,
    Table,
    and_,
    create_engine,
    func,
    select,
    update,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import declarative_base, composite

update_field_list = ["handler", "config_yaml"]
update_common_fields = [
    "prepare_script",
    "collect_script",
    "data_query",
    "coll_source",
    "coll_in",
    "coll_out",
    "status",
]


def _check_result(result) -> None:
    """Placeholder function to check on SQL query results"""
    assert result


class CMTable(CMTableBase):
    def get_handler(self) -> Handler:
        return Handler.get_handler(self.handler, self.config_yaml)

    @classmethod
    def post_insert(cls, dbi, handler, insert_fields: dict[str, Any], **kwargs):
        return None


Base = declarative_base()


class Script(Base, ScriptBase):
    __tablename__ = "script"

    id = Column(Integer, primary_key=True)  # Unique script ID
    script_url = Column(String)  # Url for script
    log_url = Column(String)  # Url for log
    config_url = Column(String)  # Url for config
    checker = Column(String)  # Checker class
    status = Column(Enum(StatusEnum))  # Status flag

    def __repr__(self):
        return f"Script {self.id}: {self.checker} {self.log_url} {self.status.name}"

    def check_status(self, conn) -> StatusEnum:
        current_status = self.status
        checker = Checker.get_checker(self.checker)
        new_status = checker.check_url(self.log_url, self.status)
        if new_status != current_status:
            stmt = update(Script).where(Script.id == self.id).values(status=new_status)
            upd_result = conn.execute(stmt)
            _check_result(upd_result)
            conn.commit()
        return new_status


class Production(Base, CMTable):
    __tablename__ = "production"

    id = Column(Integer, primary_key=True)  # Unique production ID
    name = Column(String, unique=True)  # Production Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    status = None
    db_id = composite(DbId, id)
    match_keys = [id]
    update_fields = update_field_list

    @hybrid_property
    def fullname(self):
        return self.name

    @classmethod
    def get_parent_key(cls):
        return None

    def __repr__(self):
        return f"Production {self.name} {self.db_id}: {self.handler} {self.config_yaml}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        name = handler.get_kwarg_value("production_name", **kwargs)
        insert_fields = dict(name=name)
        return insert_fields


class Campaign(Base, CMTable):
    __tablename__ = "campaign"

    id = Column(Integer, primary_key=True)  # Unique campaign ID
    p_id = Column(Integer, ForeignKey(Production.id))
    name = Column(String)  # Campaign Name
    p_name = Column(String)  # Production Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.id))
    collect_script = Column(Integer, ForeignKey(Script.id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    butler_repo = Column(String)  # URL for butler repository
    prod_base_url = Column(String)  # URL for root of the production area
    db_id = composite(DbId, p_id, id)
    match_keys = [p_id, id]
    update_fields = update_field_list + update_common_fields

    @hybrid_property
    def fullname(self):
        return self.p_name + "/" + self.name

    @classmethod
    def get_parent_key(cls):
        return cls.p_id

    def __repr__(self):
        return f"Campaign {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        if "butler_repo" not in kwargs:
            raise KeyError("butler_repo must be specified with inserting a campaign")
        if "prod_base_url" not in kwargs:
            raise KeyError("prod_base_url must be specified with inserting a campaign")
        insert_fields = dict(
            name=handler.get_kwarg_value("campaign_name", **kwargs),
            p_name=handler.get_kwarg_value("production_name", **kwargs),
            p_id=parent_db_id.p_id,
            status=StatusEnum.waiting,
            butler_repo=kwargs["butler_repo"],
            prod_base_url=kwargs["prod_base_url"],
            handler=handler.get_handler_class_name(),
            config_yaml=handler.config_url,
        )
        extra_fields = dict(
            fullname="{p_name}/{name}".format(**insert_fields),
        )
        coll_names = handler.coll_name_hook(LevelEnum.step, insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return insert_fields

    @classmethod
    def post_insert(cls, dbi, handler, insert_fields: dict[str, Any], **kwargs):
        kwcopy = kwargs.copy()
        previous_step_id = None
        coll_source = insert_fields.get("coll_in")
        parent_db_id = dbi.get_db_id(LevelEnum.campaign, **kwcopy)
        for step_name in handler.step_dict.keys():
            kwcopy.update(step_name=step_name)
            kwcopy.update(previous_step_id=previous_step_id)
            kwcopy.update(coll_source=coll_source)
            step_insert = dbi.insert(LevelEnum.step, parent_db_id, handler, **kwcopy)
            step_id = parent_db_id.extend(LevelEnum.step, step_insert["id"])
            coll_source = step_insert.get("coll_out")
            if previous_step_id is not None:
                dbi.add_prerequisite(step_id, parent_db_id.extend(LevelEnum.step, previous_step_id))
            previous_step_id = dbi.get_row_id(LevelEnum.step, **kwcopy)


class Step(Base, CMTable):
    __tablename__ = "step"

    id = Column(Integer, primary_key=True)  # Unique Step ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    name = Column(String)  # Step name
    p_name = Column(String)  # Production Name
    c_name = Column(String)  # Campaign Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.id))
    collect_script = Column(Integer, ForeignKey(Script.id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    previous_step_id = Column(Integer)
    db_id = composite(DbId, p_id, c_id, id)
    match_keys = [p_id, c_id, id]
    update_fields = update_field_list + update_common_fields

    @hybrid_property
    def fullname(self):
        return self.p_name + "/" + self.c_name + "/" + self.name

    @classmethod
    def get_parent_key(cls):
        return cls.c_id

    def __repr__(self):
        return f"Step {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        insert_fields = dict(
            name=handler.get_kwarg_value("step_name", **kwargs),
            p_name=handler.get_kwarg_value("production_name", **kwargs),
            c_name=handler.get_kwarg_value("campaign_name", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            data_query=handler.get_config_var("data_query", "", **kwargs),
            status=StatusEnum.waiting,
            handler=handler.get_handler_class_name(),
            config_yaml=handler.config_url,
        )
        extra_fields = dict(
            fullname="{p_name}/{c_name}/{name}".format(**insert_fields),
            prod_base_url=handler.get_kwarg_value("prod_base_url", **kwargs),
        )
        coll_names = handler.coll_name_hook(LevelEnum.step, insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return insert_fields


class Group(Base, CMTable):
    __tablename__ = "group"

    id = Column(Integer, primary_key=True)  # Unique Group ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    name = Column(String)  # Group name
    p_name = Column(String)  # Production Name
    c_name = Column(String)  # Campaign Name
    s_name = Column(String)  # Step Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.id))
    collect_script = Column(Integer, ForeignKey(Script.id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    db_id = composite(DbId, p_id, c_id, s_id, id)
    match_keys = [p_id, c_id, s_id, id]
    update_fields = update_field_list + update_common_fields

    @hybrid_property
    def fullname(self):
        return self.p_name + "/" + self.c_name + "/" + self.s_name + "/" + self.name

    @classmethod
    def get_parent_key(cls):
        return cls.s_id

    def __repr__(self):
        return f"Group {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        insert_fields = dict(
            name=handler.get_kwarg_value("group_name", **kwargs),
            p_name=handler.get_kwarg_value("production_name", **kwargs),
            c_name=handler.get_kwarg_value("campaign_name", **kwargs),
            s_name=handler.get_kwarg_value("step_name", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_id=parent_db_id.s_id,
            data_query=handler.get_config_var("data_query", "", **kwargs),
            coll_source=handler.get_config_var("coll_source", "", **kwargs),
            status=StatusEnum.waiting,
            handler=handler.get_handler_class_name(),
            config_yaml=handler.config_url,
        )
        extra_fields = dict(
            fullname="{p_name}/{c_name}/{s_name}/{name}".format(**insert_fields),
            prod_base_url=handler.get_kwarg_value("prod_base_url", **kwargs),
        )
        coll_names = handler.coll_name_hook(LevelEnum.group, insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return insert_fields

    @classmethod
    def post_insert(cls, dbi, handler, insert_fields: dict[str, Any], **kwargs):
        kwcopy = kwargs.copy()
        kwcopy["workflow_idx"] = kwcopy.get("workflow_idx", 0)
        coll_in = insert_fields.get("coll_in")
        kwcopy.update(coll_source=coll_in)
        parent_db_id = dbi.get_db_id(LevelEnum.group, **kwcopy)
        dbi.insert(LevelEnum.workflow, parent_db_id, handler, **kwcopy)
        dbi.prepare(LevelEnum.workflow, parent_db_id)


class Workflow(Base, CMTable):
    __tablename__ = "workflow"

    id = Column(Integer, primary_key=True)  # Unique Workflow ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    g_id = Column(Integer, ForeignKey(Group.id))
    name = Column(String)  # Index for this workflow
    p_name = Column(String)  # Production Name
    c_name = Column(String)  # Campaign Name
    s_name = Column(String)  # Step Name
    g_name = Column(String)  # Group Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.id))
    collect_script = Column(Integer, ForeignKey(Script.id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    n_tasks_all = Column(Integer, default=0)  # Number of associated tasks
    n_tasks_done = Column(Integer, default=0)  # Number of finished tasks
    n_tasks_failed = Column(Integer, default=0)  # Number of failed tasks
    n_clusters_all = Column(Integer, default=0)  # Number of associated clusters
    n_clusters_done = Column(Integer, default=0)  # Number of finished clusters
    n_clusters_failed = Column(Integer, default=0)  # Number of failed clusters
    workflow_start = Column(DateTime)  # Workflow start time
    workflow_end = Column(DateTime)  # Workflow end time
    workflow_cputime = Column(Float)
    run_script = Column(Integer, ForeignKey(Script.id))
    db_id = composite(DbId, p_id, c_id, s_id, g_id, id)
    match_keys = [p_id, c_id, s_id, g_id, id]
    update_fields = (
        update_field_list
        + update_common_fields
        + [
            "n_tasks_done",
            "n_tasks_failed",
            "n_clusters_done",
            "n_clusters_failed",
            "workflow_start",
            "workflow_end",
            "workflow_cputime",
            "run_script",
        ]
    )

    @hybrid_property
    def fullname(self):
        return self.p_name + "/" + self.c_name + "/" + self.s_name + "/" + self.g_name + "/" + self.name

    @classmethod
    def get_parent_key(cls):
        return cls.g_id

    def __repr__(self):
        return f"Workflow {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        insert_fields = dict(
            g_name=handler.get_kwarg_value("group_name", **kwargs),
            p_name=handler.get_kwarg_value("production_name", **kwargs),
            c_name=handler.get_kwarg_value("campaign_name", **kwargs),
            s_name=handler.get_kwarg_value("step_name", **kwargs),
            name="%06i" % handler.get_kwarg_value("workflow_idx", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_id=parent_db_id.s_id,
            g_id=parent_db_id.g_id,
            status=StatusEnum.waiting,
            handler=handler.get_handler_class_name(),
            config_yaml=handler.config_url,
        )
        extra_fields = dict(
            fullname="{p_name}/{c_name}/{s_name}/{g_name}/{name}".format(**insert_fields),
            prod_base_url=handler.get_kwarg_value("prod_base_url", **kwargs),
        )
        coll_names = handler.coll_name_hook(LevelEnum.workflow, insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return insert_fields


class Dependency(Base):
    __tablename__ = "dependency"

    id = Column(Integer, primary_key=True)  # Unique dependency ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    g_id = Column(Integer, ForeignKey(Group.id))
    w_id = Column(Integer, ForeignKey(Workflow.id))
    depend_p_id = Column(Integer, ForeignKey(Production.id))
    depend_c_id = Column(Integer, ForeignKey(Campaign.id))
    depend_s_id = Column(Integer, ForeignKey(Step.id))
    depend_g_id = Column(Integer, ForeignKey(Group.id))
    depend_w_id = Column(Integer, ForeignKey(Workflow.id))
    db_id = composite(DbId, p_id, c_id, s_id, g_id, w_id)
    depend_db_id = composite(DbId, depend_p_id, depend_c_id, depend_s_id, depend_g_id, depend_w_id)
    depend_keys = [depend_p_id, depend_c_id, depend_s_id, depend_g_id, depend_w_id]

    def __repr__(self):
        return f"Dependency {self.db_id}: {self.depend_db_id}"


def create_db(engine) -> None:
    """Creates a database as specific by `engine.url`

    Populates the database with empty tables
    """
    from sqlalchemy_utils import create_database  # pylint: disable=import-outside-toplevel

    create_database(engine.url)
    Base.metadata.create_all(engine)


def build_engine(db_url, **kwargs):
    """Return the sqlalchemy engine, building the database if needed"""
    from sqlalchemy_utils import database_exists  # type: ignore

    kwcopy = kwargs.copy()
    create = kwcopy.pop("create", False)
    engine = create_engine(db_url, **kwcopy)
    if not database_exists(engine.url):
        if create:
            create_db(engine)
    if not database_exists(engine.url):
        raise RuntimeError(f"Failed to access or create database {db_url}")
    return engine


def get_table(level: LevelEnum) -> Table:
    """Return the Table corresponding to a `level`"""
    all_tables = {
        LevelEnum.production: Production,
        LevelEnum.campaign: Campaign,
        LevelEnum.step: Step,
        LevelEnum.group: Group,
        LevelEnum.workflow: Workflow,
    }
    return all_tables[level]


def return_first_column(conn, sel) -> Optional[int]:
    """Returns the first column in the first row matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    try:
        return sel_result.all()[0][0]
    except IndexError:
        return None


def return_single_row(conn, sel):
    """Returns the first row matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    return sel_result.all()[0]


def return_iterable(conn, sel) -> Iterable:
    """Returns an iterable matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    for x_ in sel_result:
        yield x_[0]


def return_count(conn, count) -> int:
    """Returns the number of rows mathcing a selection"""
    count_result = conn.execute(count)
    _check_result(count_result)
    return count_result.scalar()


def return_select_count(conn, sel) -> int:
    """Counts an iterable matching a selection"""
    itr = return_iterable(conn, sel)
    n_sel = 0
    for _ in itr:
        n_sel += 1
    return n_sel


def print_select(conn, stream: TextIO, sel) -> None:
    """Prints all the rows matching a selection"""
    sel_result = conn.execute(sel)
    _check_result(sel_result)
    for row in sel_result:
        stream.write(f"{str(row)}\n")


def get_count_query(level: LevelEnum, db_id: Optional[DbId]):
    """Return the query to count rows matching an id"""
    table = get_table(level)
    count_key = table.get_parent_key()
    if count_key is None:
        return func.count(table.id)
    if db_id is not None:
        return func.count(count_key == db_id[level])
    return func.count(count_key)


def get_row_query(level: LevelEnum, db_id: DbId, columns=None):
    """Returns the selection a single row given db_id"""
    table = get_table(level)
    if columns is None:
        sel = select(table).where(table.id == db_id[level])
    else:
        sel = select(columns).where(table.id == db_id[level])
    return sel


def get_rows_with_status_query(level: LevelEnum, status: StatusEnum):
    """Returns the selection for all rows with a particular status"""
    table = get_table(level)
    sel = select([table.id]).where(table.status == status)
    return sel


def get_id_match_query(level: LevelEnum, parent_id: Optional[int], match_name: Any):
    """Returns the selection to match a particular ID"""
    table = get_table(level)
    parent_field = table.get_parent_key()
    if parent_field is None:
        sel = select([table.id]).where(table.name == match_name)
    else:
        sel = select([table.id]).where(and_(parent_field == parent_id, table.name == match_name))
    return sel


def get_match_query(level: LevelEnum, db_id: DbId):
    """Returns the selection all rows given db_id at a given level"""
    table = get_table(level)
    if db_id is None:
        id_tuple = ()
    else:
        id_tuple = db_id.to_tuple()[0 : level.value + 1]
    parent_key = None
    row_id = None
    for i, row_id_ in enumerate(id_tuple):
        if row_id_ is not None:
            parent_key = table.match_keys[i]
            row_id = row_id_
    if parent_key is None:
        sel = select(table)
    else:
        sel = select(table).where(parent_key == row_id)
    return sel


def add_prerequisite(conn, depend_id: DbId, prereq_id: DbId):
    """Inserts a dependency"""
    conn.add(
        Dependency(
            p_id=prereq_id[LevelEnum.production],
            c_id=prereq_id[LevelEnum.campaign],
            s_id=prereq_id[LevelEnum.step],
            g_id=prereq_id[LevelEnum.group],
            w_id=prereq_id[LevelEnum.workflow],
            depend_p_id=depend_id[LevelEnum.production],
            depend_c_id=depend_id[LevelEnum.campaign],
            depend_s_id=depend_id[LevelEnum.step],
            depend_g_id=depend_id[LevelEnum.group],
            depend_w_id=depend_id[LevelEnum.workflow],
        )
    )
    conn.commit()


def get_prerequisites(conn, level: LevelEnum, db_id: DbId):
    sel = select(Dependency).where(Dependency.depend_keys[level.value] == db_id[level])
    itr = return_iterable(conn, sel)
    db_id_list = [row_.db_id for row_ in itr]
    return db_id_list


def add_script(conn, **kwargs) -> int:
    """Insert a new row with details about a script"""
    script = Script(**kwargs)
    conn.add(script)
    conn.commit()
    counter = func.count(Script.id)
    return return_count(conn, counter)


def get_script(conn, script_id: int) -> ScriptBase:
    """Get a particular script by id"""
    sel = select(Script).where(Script.id == script_id)
    return return_single_row(conn, sel)[0]


def insert_values(conn, level: LevelEnum, **kwargs):
    """Inserts a new row at a given level with values given in kwargs"""
    table = get_table(level)
    conn.add(table(**kwargs))
    conn.commit()


def update_values(conn, level: LevelEnum, db_id: DbId, **kwargs):
    """Updates a given row with values given in kwargs"""
    table = get_table(level)
    stmt = update(table).where(table.id == db_id[level]).values(**kwargs)
    upd_result = conn.execute(stmt)
    _check_result(upd_result)
    conn.commit()
