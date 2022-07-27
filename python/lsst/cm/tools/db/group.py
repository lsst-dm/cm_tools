from typing import Any, Iterable

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import InputType, LevelEnum, OutputType, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Group(common.Base, common.CMTable):
    __tablename__ = "group"

    level = LevelEnum.group
    id = Column(Integer, primary_key=True)  # Unique Group ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    name = Column(String)  # Group name
    fullname = Column(String, unique=True)  # Unique name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    input_type = Column(Enum(InputType))  # How to manage input data
    output_type = Column(Enum(OutputType))  # How to manage output data
    status = Column(Enum(StatusEnum))  # Status flag
    superseeded = Column(Boolean)  # Has this been superseeded
    db_id: DbId = composite(DbId, p_id, c_id, s_id, id)
    p_: Production = relationship("Production", foreign_keys=[p_id])
    c_: Campaign = relationship("Campaign", back_populates="g_")
    s_: Step = relationship("Step", back_populates="g_")
    w_: Iterable = relationship("Workflow", back_populates="g_")
    scripts_: Iterable = relationship("Script", back_populates="g_")
    depend_: Iterable = relationship("Dependency", back_populates="g_")

    match_keys = [p_id, c_id, s_id, id]
    update_fields = common.update_field_list + common.update_common_fields

    @hybrid_property
    def butler_repo(self) -> Any:
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self) -> Any:
        return self.c_.prod_base_url

    @hybrid_property
    def parent_id(self) -> Any:
        return self.s_id

    def __repr__(self) -> str:
        return f"Group {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"
