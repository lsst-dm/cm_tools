from lsst.cm.tools.core.db_interface import ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, ScriptMethod, ScriptType, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow import Workflow
from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import composite, relationship


class Script(common.Base, common.SQLScriptMixin, ScriptBase):
    """Database table to manage processing `Script`

    A `Script` is a small shell script that helps with
    collection and file management, or does stuff that
    we want to run independently.
    """

    __tablename__ = "script"

    id = Column(Integer, primary_key=True)  # Unique script ID
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    g_id = Column(Integer, ForeignKey(Group.id))
    w_id = Column(Integer, ForeignKey(Workflow.id))
    name = Column(String)  # Name for this script
    idx = Column(Integer)  # ID from this script
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    script_url = Column(String)  # Url for script
    stamp_url = Column(String)  # Url for a status 'stamp' file
    log_url = Column(String)  # Url for log
    coll_out = Column(String)  # Output collection
    checker = Column(String)  # Checker class
    rollback = Column(String)  # Rollback class
    status = Column(Enum(StatusEnum))  # Status flag
    superseeded = Column(Boolean)  # Has this been superseeded
    script_type = Column(Enum(ScriptType))  # What sort of thing the script does
    script_method = Column(Enum(ScriptMethod))  # How the script is invoked
    level = Column(Enum(LevelEnum))
    db_id: DbId = composite(DbId, c_id=c_id, s_id=s_id, g_id=g_id, w_id=w_id)
    c_: Campaign = relationship("Campaign", back_populates="scripts_")
    s_: Step = relationship("Step", back_populates="scripts_")
    g_: Group = relationship("Group", back_populates="scripts_")
    w_: Workflow = relationship("Workflow", back_populates="scripts_")

    def __repr__(self) -> str:
        return f"Script {self.id}: {self.db_id} {self.name} {self.log_url} {self.status.name}"
