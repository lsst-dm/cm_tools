from __future__ import annotations

from typing import Any, Iterable, TextIO

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Workflow(common.Base, common.CMTable):
    """Database table to manage processing `Workflow`

    A `Workflow` is a single batch submission workflow.
    This can include a very large number of individual jobs.
    """

    __tablename__ = "workflow"

    level = LevelEnum.workflow

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
    status = Column(Enum(StatusEnum))  # Status flag
    superseeded = Column(Boolean)  # Has this been superseeded
    db_id: DbId = composite(DbId, p_id, c_id, s_id, g_id, id)
    p_: Production = relationship("Production", foreign_keys=[p_id])
    c_: Campaign = relationship("Campaign", back_populates="w_")
    s_: Step = relationship("Step", back_populates="w_")
    g_: Group = relationship("Group", back_populates="w_")
    scripts_: Iterable = relationship("Script", back_populates="w_")
    jobs_: Iterable = relationship("Job", back_populates="w_")
    depend_: Iterable = relationship("Dependency", back_populates="w_")

    match_keys = [p_id, c_id, s_id, g_id, id]

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

    def print_tree(self, stream: TextIO) -> None:
        stream.write(f"  {self}\n")
        for job in self.jobs_:
            stream.write(f"    -{job}\n")

    def sub_iterators(self) -> list[Iterable]:
        return []
