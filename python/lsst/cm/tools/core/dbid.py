from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from lsst.cm.tools.core.utils import LevelEnum


@dataclass
class DbId:
    """Information to identify a single entry in the CM database tables

    This consist of primary keys into each of the tables

    Notes
    -----
    A DbId can be used either to specific a single entry or to specify
    a set of entries.

    At each level, the DbId can either have a primary key, or `None`
    any matching function should specify which table to search for
    matches.

    If matching is requested at a level which the DbId has a primary
    key for, the matching should return only that one DbId.

    On the other hand, the DbId does not have a primary key at that
    level, the match should return all the entries that match
    at the highest level it does have a key.  E.g., asking for matching
    `LevelEnum.group` for a DbId that only contains `LevelEnum.step`
    will match all the `LevelEnum.group` objects with that `LevelEnum.step`
    """

    p_id: Optional[int] = None
    c_id: Optional[int] = None
    s_id: Optional[int] = None
    g_id: Optional[int] = None
    w_id: Optional[int] = None

    def level(self) -> Optional[LevelEnum]:
        """Return the highest level which we do have a specific key for"""
        if self.w_id is not None:
            return LevelEnum.workflow
        if self.g_id is not None:
            return LevelEnum.group
        if self.s_id is not None:
            return LevelEnum.step
        if self.c_id is not None:
            return LevelEnum.campaign
        if self.p_id is not None:
            return LevelEnum.production
        return None

    def to_tuple(self) -> tuple:
        """Return data as tuple"""
        return (self.p_id, self.c_id, self.s_id, self.g_id, self.w_id)

    def __getitem__(self, level: LevelEnum) -> int:
        """Return primary key at a particular level"""
        return self.to_tuple()[level.value]

    def __repr__(self) -> str:
        return f"DbId({self.p_id}:{self.c_id}:{self.s_id}:{self.g_id}:{self.w_id})".replace("None", "x")

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
