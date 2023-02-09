from typing import Any, Iterable

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.db import common


class Production(common.Base, common.CMTable):
    """Database table to manage processing `Production`

    A `Production` is just a bunch of associated `Campaign`
    Really this is just a useful way of collected related
    `Campaign`.
    """

    __tablename__ = "production"

    id = Column(Integer, primary_key=True)  # Unique production ID
    name = Column(String, unique=True)  # Production Name
    status = None
    db_id: DbId = composite(DbId, id)
    c_: Iterable = relationship("Campaign", back_populates="p_")

    match_keys = [id]
    parent_id = None
    parent_ = None

    @hybrid_property
    def p_id(self) -> Any:
        """Maps id to p_id for consistency"""
        return self.id

    @hybrid_property
    def fullname(self) -> Any:
        """Maps name to fullname for consistency"""
        return self.name

    def __repr__(self) -> str:
        return f"Production {self.fullname} {self.p_id} {self.db_id}"
