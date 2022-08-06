from typing import Any, Iterable, TextIO

from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import InputType, LevelEnum, OutputType, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.production import Production


class Step(common.Base, common.CMTable):
    """Database table to manage processing `Step`

    A `Step` consists of several processing `Group` which
    are run in parallel

    """

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
    coll_validate = Column(String)  # Validate data collection
    input_type = Column(Enum(InputType))  # How to manage input data
    output_type = Column(Enum(OutputType))  # How to manage output data
    status = Column(Enum(StatusEnum))  # Status flag
    superseded = Column(Boolean)  # Has this been superseded
    previous_step_id = Column(Integer)
    db_id: DbId = composite(DbId, p_id, c_id, id)
    p_: Production = relationship("Production", foreign_keys=[p_id])
    c_: Campaign = relationship("Campaign", back_populates="s_")
    g_: Iterable = relationship("Group", back_populates="s_")
    w_: Iterable = relationship("Workflow", back_populates="s_")
    scripts_: Iterable = relationship("Script", back_populates="s_")
    jobs_: Iterable = relationship("Job", back_populates="s_")
    depend_: Iterable = relationship("Dependency", back_populates="s_")

    match_keys = [p_id, c_id, id]

    @hybrid_property
    def butler_repo(self) -> Any:
        """Direct access to the butler_repo URL, for convinience"""
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self) -> Any:
        """Direct access to the production area URL, for convinience"""
        return self.c_.prod_base_url

    @hybrid_property
    def root_coll(self) -> Any:
        """Direct access to the root of collection names, for convinience"""
        return self.c_.root_coll

    @hybrid_property
    def parent_id(self) -> Any:
        """Maps c_id to parent_id for consistency"""
        return self.c_id

    def __repr__(self) -> str:
        if self.superseded:
            supersede_string = "SUPERSEDED"
        else:
            supersede_string = ""
        return f"Step {self.fullname} {self.db_id}: {self.handler} {self.status.name} {supersede_string}"

    def print_tree(self, stream: TextIO) -> None:
        """Print entry in tree-like format"""
        stream.write(f"  {self}\n")
        for script in self.scripts_:
            stream.write(f"    -{script}\n")
        for group in self.g_:
            group.print_tree(stream)

    def children(self) -> Iterable:
        """Maps self.g_ to self.children() for consistency"""
        for group in self.g_:
            yield group

    def sub_iterators(self) -> list[Iterable]:
        """Iterators over sub-entries, used for recursion"""
        return [self.w_, self.g_]
