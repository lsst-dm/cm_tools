from typing import Any

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum
from lsst.cm.tools.db import common
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import Column, String  # type: ignore
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite


class Production(common.Base, common.CMTable):
    __tablename__ = "production"

    level = LevelEnum.production
    id = Column(Integer, primary_key=True)  # Unique production ID
    name = Column(String, unique=True)  # Production Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    status = None
    db_id = composite(DbId, id)
    match_keys = [id]
    update_fields = common.update_field_list

    @hybrid_property
    def fullname(self):
        return self.name

    @classmethod
    def get_parent_key(cls):
        return None

    def __repr__(self):
        return f"Production {self.fullname} {self.db_id}: {self.handler} {self.config_yaml}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        name = handler.get_kwarg_value("production_name", **kwargs)
        insert_fields = dict(name=name)
        return insert_fields
