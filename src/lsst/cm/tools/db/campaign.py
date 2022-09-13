from typing import Any, Iterable, TextIO

from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import InputType, LevelEnum, OutputType, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.production import Production


class Campaign(common.Base, common.CMTable):
    """Database table to manage processing `Campaign`

    A `Campaign` consists of several processing `Step` which
    are run sequentially

    `Campaign` is also where we keep the global configuration
    such as the URL for the butler repo and the production area
    """

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
    coll_validate = Column(String)  # Validate data collection
    coll_ancil = Column(String)  # Ancillary (i.e., calibration) collection
    input_type = Column(Enum(InputType))  # How to manage input data
    output_type = Column(Enum(OutputType))  # How to manage output data
    status = Column(Enum(StatusEnum))  # Status flag
    superseded = Column(Boolean, default=False)  # Has this been superseded
    butler_repo = Column(String)  # URL for butler repository
    root_coll = Column(String)  # root for collection names
    prod_base_url = Column(String)  # URL for root of the production area
    db_id: DbId = composite(DbId, p_id, id)
    p_: Iterable = relationship("Production", back_populates="c_")
    s_: Iterable = relationship("Step", back_populates="c_")
    g_: Iterable = relationship("Group", back_populates="c_")
    w_: Iterable = relationship("Workflow", back_populates="c_")
    scripts_: Iterable = relationship("Script", back_populates="c_")
    jobs_: Iterable = relationship("Job", back_populates="c_")
    depend_: Iterable = relationship("Dependency", back_populates="c_")

    match_keys = [p_id, id]

    @hybrid_property
    def parent_id(self) -> Any:
        """Maps p_id to parent_id for consistency"""
        return self.p_id

    def __repr__(self) -> str:
        if self.superseded:
            supersede_string = "SUPERSEDED"
        else:
            supersede_string = ""
        return f"Campaign {self.fullname} {self.db_id}: {self.handler} {self.status.name} {supersede_string}"

    def print_tree(self, stream: TextIO) -> None:
        """Print entry in tree-like format"""
        stream.write(f"{self}\n")
        for script in self.scripts_:
            stream.write(f"  -{script}\n")
        for step in self.s_:
            step.print_tree(stream)

    def children(self) -> Iterable:
        """Maps self.s_ to self.children() for consistency"""
        for step in self.s_:
            yield step

    def sub_iterators(self) -> list[Iterable]:
        """Iterators over sub-entries, used for recursion"""
        return [self.w_, self.g_, self.s_]
