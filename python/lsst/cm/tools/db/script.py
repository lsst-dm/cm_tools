from lsst.cm.tools.core.db_interface import ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, ScriptMethod, ScriptType, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import composite, relationship


class Script(common.Base, common.SQLScriptMixin, ScriptBase):
    __tablename__ = "script"

    id = Column(Integer, primary_key=True)  # Unique script ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    g_id = Column(Integer, ForeignKey(Group.id))
    name = Column(String)  # Name for this script
    idx = Column(Integer)  # ID from this script
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
    db_id: DbId = composite(DbId, p_id, c_id, s_id, g_id)
    c_: Campaign = relationship("Campaign", back_populates="scripts_")
    s_: Step = relationship("Step", back_populates="scripts_")
    g_: Group = relationship("Group", back_populates="scripts_")

    update_fields = [
        "checker",
        "rollback",
        "status",
        "script_method",
    ]

    def __repr__(self) -> str:
        return f"Script {self.id}: {self.checker} {self.log_url} {self.status.name}"
