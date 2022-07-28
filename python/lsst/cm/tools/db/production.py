from typing import Any, Iterable

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.db import common
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Production(common.Base, common.CMTable):
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
        return self.id

    @hybrid_property
    def fullname(self) -> Any:
        return self.name

    def __repr__(self) -> str:
        return f"Production {self.fullname} {self.p_id} {self.db_id}"
