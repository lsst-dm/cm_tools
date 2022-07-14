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

from __future__ import annotations  # Needed for class member returning class

from dataclasses import dataclass
from typing import Optional

from lsst.cm.tools.core.utils import LevelEnum


@dataclass
class DbId:
    """Information to identify a single entry in the CM database tables

    This consist of primary keys into each of the tables
    """

    p_id: Optional[int] = None
    c_id: Optional[int] = None
    s_id: Optional[int] = None
    g_id: Optional[int] = None
    w_id: Optional[int] = None

    @classmethod
    def create_from_row(cls, row) -> DbId:
        ids = {}
        for key in ["p_id", "c_id", "s_id", "g_id", "w_id"]:
            try:
                ids[key] = row[key]
            except KeyError:
                ids[key] = None
        return cls(**ids)

    def to_tuple(self) -> tuple:
        """Return data as tuple"""
        return (self.p_id, self.c_id, self.s_id, self.g_id, self.w_id)

    def __getitem__(self, level: LevelEnum) -> int:
        """Return primary key at a particular level"""
        return self.to_tuple()[level.value]

    def extend(self, level: LevelEnum, row_id: int) -> DbId:
        """Return an extension of this DbId

        This adds a primary key at a particular level

        Parameters
        ----------
        level : LevelEnum
            The level were are adding the key at

        row_id : int
            The primary key we are adding

        Returns
        -------
        new_id : DbId
            The extended DbId
        """
        if level == LevelEnum.production:
            return DbId(p_id=row_id)
        if level == LevelEnum.campaign:
            return DbId(p_id=self.p_id, c_id=row_id)
        if level == LevelEnum.step:
            return DbId(p_id=self.p_id, c_id=self.c_id, s_id=row_id)
        if level == LevelEnum.group:
            return DbId(p_id=self.p_id, c_id=self.c_id, s_id=self.s_id, g_id=row_id)
        return DbId(p_id=self.p_id, c_id=self.c_id, s_id=self.s_id, g_id=self.g_id, w_id=row_id)
