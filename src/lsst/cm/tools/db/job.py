from typing import Iterable

from sqlalchemy import Boolean, Column, DateTime, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.orm import composite, relationship

from lsst.cm.tools.core.db_interface import ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, ScriptMethod, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.config import Fragment
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow import Workflow


class Job(common.Base, common.SQLScriptMixin, ScriptBase):
    """Database table to manage processing `Job`

    A `Job` launchs job on a batch system
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)  # Unique script ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    g_id = Column(Integer, ForeignKey(Group.id))
    w_id = Column(Integer, ForeignKey(Workflow.id))
    frag_id = Column(Integer, ForeignKey(Fragment.id))
    name = Column(String)  # Name for this script
    idx = Column(Integer)  # ID from this script
    bps_yaml_template = Column(String)  # Template file for bps yaml
    bps_script_template = Column(String)  # Template file for bps script
    pipeline_yaml = Column(String)  # Path to pipeline yaml file
    lsst_version = Column(String)  # Version of LSST software stack
    lsst_custom_setup = Column(String)  # Custom setup for LSST software
    script_url = Column(String)  # Url for script
    stamp_url = Column(String)  # Url for a status 'stamp' file
    panda_url = Column(String)  # Url for a panda file
    log_url = Column(String)  # Url for log
    config_url = Column(String)  # Url for script configuration
    json_url = Column(String)  # Url for json file
    coll_out = Column(String)  # Output collection
    checker = Column(String)  # Checker class
    rollback = Column(String)  # Rollback class
    status = Column(Enum(StatusEnum), default=StatusEnum.waiting)  # Status flag
    batch_status = Column(String)  # Status as returned by batch system
    panda_status = Column(String)  # Status as returned by panda
    superseded = Column(Boolean)  # Has this been superseded
    script_method = Column(Enum(ScriptMethod))  # How the script is invoked
    n_tasks_all = Column(Integer, default=0)  # Number of associated tasks
    n_tasks_done = Column(Integer, default=0)  # Number of finished tasks
    n_tasks_failed = Column(Integer, default=0)  # Number of failed tasks
    n_clusters_all = Column(Integer, default=0)  # Number of associated clusters
    n_clusters_done = Column(Integer, default=0)  # Number of finished clusters
    n_clusters_failed = Column(Integer, default=0)  # Number of failed clusters
    job_start = Column(DateTime)  # Workflow start time
    job_end = Column(DateTime)  # Workflow end time
    job_cputime = Column(Float)

    level = Column(Enum(LevelEnum))
    db_id: DbId = composite(DbId, p_id, c_id, s_id, g_id, w_id)
    c_: Campaign = relationship("Campaign", back_populates="jobs_")
    s_: Step = relationship("Step", back_populates="jobs_")
    g_: Group = relationship("Group", back_populates="jobs_")
    w_: Workflow = relationship("Workflow", back_populates="jobs_")
    frag_: Fragment = relationship("Fragment", viewonly=True)
    errors_: Iterable = relationship("ErrorInstance", back_populates="job_")

    def __repr__(self) -> str:
        if self.superseded:
            supersede_string = "SUPERSEDED"
        else:
            supersede_string = ""
        string_out = f"BatchJob {self.id}: {self.db_id} {self.name} {self.frag_}"
        string_out += f" {self.status.name} {self.checker} {self.stamp_url} {self.panda_url}"
        string_out += f" {self.panda_status} {self.w_.id}"
        string_out += f" {supersede_string}"
        return string_out
