from __future__ import annotations

from typing import Any, Iterable, TextIO

from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.config import Config, Fragment
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step


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
    config_id = Column(Integer, ForeignKey(Config.id))
    frag_id = Column(Integer, ForeignKey(Fragment.id))
    idx = Column(Integer)  # Index from this work
    name = Column(String)  # Index for this workflow
    fullname = Column(String, unique=True)  # Unique name
    data_query = Column(String)  # Data query
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    coll_validate = Column(String)  # Validate data collection
    status = Column(Enum(StatusEnum), default=StatusEnum.waiting)  # Status flag
    superseded = Column(Boolean, default=False)  # Has this been superseded
    db_id: DbId = composite(DbId, p_id, c_id, s_id, g_id, id)
    p_: Production = relationship("Production", foreign_keys=[p_id])
    c_: Campaign = relationship("Campaign", back_populates="w_")
    s_: Step = relationship("Step", back_populates="w_")
    g_: Group = relationship("Group", back_populates="w_")
    config_: Config = relationship("Config", viewonly=True)
    frag_: Fragment = relationship("Fragment", viewonly=True)
    all_scripts_: Iterable = relationship("Script", back_populates="w_")
    scripts_: Iterable = relationship(
        "Script",
        primaryjoin="and_(Workflow.id==Script.w_id, Script.level=='workflow')",
        viewonly=True,
    )
    jobs_: Iterable = relationship("Job", back_populates="w_")
    depend_: Iterable = relationship("Dependency", back_populates="w_")

    match_keys = [p_id, c_id, s_id, g_id, id]

    @hybrid_property
    def butler_repo(self) -> Any:
        """Direct access to the butler_repo URL, for convinience"""
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self) -> Any:
        """Direct access to the production area URL, for convinience"""
        return self.c_.prod_base_url

    @hybrid_property
    def parent_id(self) -> Any:
        """Maps g_id to parent_id for consistency"""
        return self.g_id

    def __repr__(self) -> str:
        if self.superseded:
            supersede_string = "SUPERSEDED"
        else:
            supersede_string = ""
        return (
            f"Workflow {self.fullname} {self.db_id}: {self.frag_id} {self.status.name} {supersede_string}\n"
        )

    def print_tree(self, stream: TextIO) -> None:
        """Print entry in tree-like format"""
        stream.write(f"  {self}\n")
        for job in self.jobs_:
            stream.write(f"    -{job}\n")

    def children(self) -> Iterable:
        """Maps empty list to self.children() for consistency"""
        return []

    def sub_iterators(self) -> list[Iterable]:
        """Iterators over sub-entries, used for recursion"""
        return []
