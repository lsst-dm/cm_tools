from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.script import Script
from lsst.cm.tools.db.step import Step
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import Column, DateTime, Enum, Float, ForeignKey, String  # type: ignore
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Workflow(common.Base, common.CMTable):
    __tablename__ = "workflow"

    level = LevelEnum.workflow
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
    p_ = relationship("Production", foreign_keys=[p_id])
    c_ = relationship("Campaign", foreign_keys=[c_id])
    s_ = relationship("Step", foreign_keys=[s_id])
    g_ = relationship("Group", foreign_keys=[g_id])

    match_keys = [p_id, c_id, s_id, g_id, id]
    update_fields = (
        common.update_field_list
        + common.update_common_fields
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

    @hybrid_property
    def butler_repo(self):
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self):
        return self.c_.prod_base_url

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
            data_query=handler.get_config_var("data_query", "", **kwargs),
            coll_source=handler.get_config_var("coll_source", "", **kwargs),
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

    def launch(self, dbi: DbInterface):
        script = dbi.get_script(self.run_script)
        config_url = script.config_url
        script_url = script.script_url
        submit_command = f"{script_url} {config_url}"
        # workflow_start = datetime.now()
        print(f"Submitting workflow {str(self.db_id)} with {submit_command}")
        update_fields = dict(status=StatusEnum.running)
        self.update_values(dbi, self.db_id, **update_fields)
