from typing import Any, Iterable, TextIO

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import InputType, LevelEnum, OutputType, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.production import Production
from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Step(common.Base, common.CMTable):
    __tablename__ = "step"

    level = LevelEnum.step
    id = Column(Integer, primary_key=True)  # Unique Step ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    name = Column(String)  # Step name
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
    previous_step_id = Column(Integer)
    db_id: DbId = composite(DbId, p_id, c_id, id)
    p_: Production = relationship("Production", foreign_keys=[p_id])
    c_: Campaign = relationship("Campaign", back_populates="s_")
    g_: Iterable = relationship("Group", back_populates="s_")
    w_: Iterable = relationship("Workflow", back_populates="s_")
    scripts_: Iterable = relationship("Script", back_populates="s_")
    depend_: Iterable = relationship("Dependency", back_populates="s_")

    match_keys = [p_id, c_id, id]
    update_fields = common.update_field_list + common.update_common_fields

    @hybrid_property
    def butler_repo(self) -> Any:
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self) -> Any:
        return self.c_.prod_base_url

    @hybrid_property
    def parent_id(self) -> Any:
        return self.c_id

    def __repr__(self) -> str:
        return f"Step {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    def print_tree(self, stream: TextIO) -> None:
        stream.write(f"  {self}\n")
        for script in self.scripts_:
            stream.write(f"    -{script}\n")
        for group in self.g_:
            group.print_tree(stream)
