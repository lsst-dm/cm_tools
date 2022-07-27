from __future__ import annotations

from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface, WorkflowBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import ScriptMethod, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from sqlalchemy import Boolean, Column, DateTime, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Workflow(common.Base, common.SQLScriptMixin, WorkflowBase):
    __tablename__ = "workflow"

    id = Column(Integer, primary_key=True)  # Unique Workflow ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    g_id = Column(Integer, ForeignKey(Group.id))
    idx = Column(Integer)  # Index from this work
    name = Column(String)  # Index for this workflow
    fullname = Column(String, unique=True)  # Unique name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    data_query = Column(String)  # Data query
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    checker = Column(String)  # Checker class
    rollback = Column(String)  # Rollback class
    status = Column(Enum(StatusEnum))  # Status flag
    superseeded = Column(Boolean)  # Has this been superseeded
    script_method = Column(Enum(ScriptMethod))  # How the script is invoked
    script_url = Column(String)  # Url for script
    stamp_url = Column(String)  # Url for a status 'stamp' file
    log_url = Column(String)  # Url for log
    config_url = Column(String)  # Url for config
    n_tasks_all = Column(Integer, default=0)  # Number of associated tasks
    n_tasks_done = Column(Integer, default=0)  # Number of finished tasks
    n_tasks_failed = Column(Integer, default=0)  # Number of failed tasks
    n_clusters_all = Column(Integer, default=0)  # Number of associated clusters
    n_clusters_done = Column(Integer, default=0)  # Number of finished clusters
    n_clusters_failed = Column(Integer, default=0)  # Number of failed clusters
    workflow_start = Column(DateTime)  # Workflow start time
    workflow_end = Column(DateTime)  # Workflow end time
    workflow_cputime = Column(Float)
    db_id: DbId = composite(DbId, p_id, c_id, s_id, g_id)
    p_: Production = relationship("Production", foreign_keys=[p_id])
    c_: Campaign = relationship("Campaign", back_populates="w_")
    s_: Step = relationship("Step", back_populates="w_")
    g_: Group = relationship("Group", back_populates="w_")

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
    def butler_repo(self) -> Any:
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self) -> Any:
        return self.c_.prod_base_url

    @hybrid_property
    def parent_id(self) -> Any:
        return self.g_id

    def __repr__(self) -> str:
        return f"Workflow {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    def get_handler(self) -> Handler:
        return Handler.get_handler(self.handler, self.config_yaml)

    def launch(self, dbi: DbInterface) -> None:
        submit_command = f"{self.script_url} {self.config_url}"
        # workflow_start = datetime.now()
        print(f"Submitting workflow {str(self.db_id)} with {submit_command}")
        update_fields = dict(status=StatusEnum.pending)
        self.update_values(dbi, self.id, **update_fields)
