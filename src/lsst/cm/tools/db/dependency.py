from __future__ import annotations

from typing import Iterable

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import composite, relationship

from lsst.cm.tools.core.db_interface import DbInterface, DependencyBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow import Workflow


class Dependency(DependencyBase, common.Base):
    """Database table to manage processing `Dependency`

    A Dependency will prevent the `depend_id` entry
    from running until the `prereq` entry is accepted
    """

    __tablename__ = "dependency"
    __allow_unmapped__ = True

    id = Column(Integer, primary_key=True)  # Unique dependency ID
    p_id = Column(Integer)
    c_id = Column(Integer)
    s_id = Column(Integer)
    g_id = Column(Integer)
    w_id = Column(Integer)
    depend_p_id = Column(Integer, ForeignKey(Production.id))
    depend_c_id = Column(Integer, ForeignKey(Campaign.id))
    depend_s_id = Column(Integer, ForeignKey(Step.id))
    depend_g_id = Column(Integer, ForeignKey(Group.id))
    depend_w_id = Column(Integer, ForeignKey(Workflow.id))
    c_: Iterable = relationship("Campaign", back_populates="depend_")
    s_: Iterable = relationship("Step", back_populates="depend_")
    g_: Iterable = relationship("Group", back_populates="depend_")
    w_: Iterable = relationship("Workflow", back_populates="depend_")
    db_id: DbId = composite(DbId, p_id, c_id, s_id, g_id, w_id)
    depend_db_id: DbId = composite(DbId, depend_p_id, depend_c_id, depend_s_id, depend_g_id, depend_w_id)
    depend_keys = [depend_p_id, depend_c_id, depend_s_id, depend_g_id]

    def __repr__(self) -> str:
        return f"Dependency {self.db_id}: {self.depend_db_id}"

    @classmethod
    def add_prerequisite(cls, dbi: DbInterface, depend_id: DbId, prereq_id: DbId) -> DependencyBase:
        """Inserts a dependency"""
        conn = dbi.connection()
        depend = cls(
            p_id=prereq_id[LevelEnum.production],
            c_id=prereq_id[LevelEnum.campaign],
            s_id=prereq_id[LevelEnum.step],
            g_id=prereq_id[LevelEnum.group],
            depend_p_id=depend_id[LevelEnum.production],
            depend_c_id=depend_id[LevelEnum.campaign],
            depend_s_id=depend_id[LevelEnum.step],
            depend_g_id=depend_id[LevelEnum.group],
        )
        conn.add(depend)
        return depend
