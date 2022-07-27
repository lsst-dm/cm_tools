from typing import Any, Iterable, TextIO

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import InputType, LevelEnum, OutputType, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.production import Production
from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Campaign(common.Base, common.CMTable):
    __tablename__ = "campaign"

    level = LevelEnum.campaign
    id = Column(Integer, primary_key=True)  # Unique campaign ID
    p_id = Column(Integer, ForeignKey(Production.id))
    name = Column(String)  # Campaign Name
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
    butler_repo = Column(String)  # URL for butler repository
    prod_base_url = Column(String)  # URL for root of the production area
    db_id: DbId = composite(DbId, p_id, id)
    p_: Iterable = relationship("Production", back_populates="c_")
    s_: Iterable = relationship("Step", back_populates="c_")
    g_: Iterable = relationship("Group", back_populates="c_")
    w_: Iterable = relationship("Workflow", back_populates="c_")
    scripts_: Iterable = relationship("Script", back_populates="c_")
    depend_: Iterable = relationship("Dependency", back_populates="c_")

    match_keys = [p_id, id]
    update_fields = common.update_field_list + common.update_common_fields

    @hybrid_property
    def parent_id(self) -> Any:
        return self.p_id

    def __repr__(self) -> str:
        return f"Campaign {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    def print_tree(self, stream: TextIO) -> None:
        stream.write(f"{self}\n")
        for script in self.scripts_:
            stream.write(f"  -{script}\n")
        for step in self.s_:
            step.print_tree(stream)
