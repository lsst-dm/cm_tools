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

from __future__ import annotations

from lsst.cm.tools.core.db_interface import DbInterface, DependencyBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow import Workflow
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import Column, ForeignKey, func, select  # type: ignore
from sqlalchemy.orm import composite


class Dependency(DependencyBase, common.Base):
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

    @classmethod
    def add_prerequisite(cls, dbi: DbInterface, depend_id: DbId, prereq_id: DbId) -> DependencyBase:
        """Inserts a dependency"""
        counter = func.count(cls.id)
        conn = dbi.connection()
        next_id = common.return_count(conn, counter) + 1
        depend = cls(
            id=next_id,
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
        conn.add(depend)
        conn.commit()
        return depend

    @classmethod
    def get_prerequisites(cls, dbi: DbInterface, db_id: DbId) -> list[DbId]:
        level = db_id.level()
        sel = select(Dependency).where(Dependency.depend_keys[level.value] == db_id[level])
        itr = common.return_iterable(dbi, sel)
        db_id_list = [row_.db_id for row_ in itr if row_.db_id.level() == level]
        return db_id_list
