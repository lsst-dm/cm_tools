from typing import Any

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.script import Script
from sqlalchemy import Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Step(common.Base, common.CMTable):
    __tablename__ = "step"

    level = LevelEnum.step
    id = Column(Integer, primary_key=True)  # Unique Step ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    name = Column(String)  # Step name
    p_name = Column(String)  # Production Name
    c_name = Column(String)  # Campaign Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.id))
    collect_script = Column(Integer, ForeignKey(Script.id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    previous_step_id = Column(Integer)
    db_id = composite(DbId, p_id, c_id, id)
    p_ = relationship("Production", foreign_keys=[p_id])
    c_ = relationship("Campaign", foreign_keys=[c_id])

    match_keys = [p_id, c_id, id]
    update_fields = common.update_field_list + common.update_common_fields

    @hybrid_property
    def fullname(self):
        return self.p_name + "/" + self.c_name + "/" + self.name

    @hybrid_property
    def butler_repo(self):
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self):
        return self.c_.prod_base_url

    @classmethod
    def get_parent_key(cls):
        return cls.c_id

    def __repr__(self):
        return f"Step {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        insert_fields = dict(
            name=handler.get_kwarg_value("step_name", **kwargs),
            p_name=handler.get_kwarg_value("production_name", **kwargs),
            c_name=handler.get_kwarg_value("campaign_name", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            coll_source=handler.get_kwarg_value("coll_source", **kwargs),
            data_query=handler.get_kwarg_value("data_query", **kwargs),
            status=StatusEnum.waiting,
            handler=handler.get_handler_class_name(),
            config_yaml=handler.config_url,
        )
        extra_fields = dict(
            fullname="{p_name}/{c_name}/{name}".format(**insert_fields),
            prod_base_url=handler.get_kwarg_value("prod_base_url", **kwargs),
        )
        coll_names = handler.coll_name_hook(LevelEnum.step, insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return insert_fields
