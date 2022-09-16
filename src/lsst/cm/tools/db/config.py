from typing import Iterable

from sqlalchemy import JSON, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.db import common


class Fragment(common.Base):
    """Database table to manage configuration fragments

    A 'Fragment" is fragment of a yaml file that has some configuration data.
    """

    __tablename__ = "fragment"

    id = Column(Integer, primary_key=True)  # Unique fragment ID
    name = Column(String)  # Name associated to the file this fragement came from
    tag = Column(String)  # Yaml tag for this fragement
    fullname = Column(String, unique=True)  # Unique name
    handler = Column(String)  # Handler class
    data = Column(JSON)  # Dictionary of configuration options
    assocs_: Iterable = relationship("ConfigAssociation", back_populates="frag_")

    def __repr__(self) -> str:
        return f"Fragment {self.id}: {self.name} {self.tag} {self.handler}"

    def get_handler(self) -> Handler:
        return Handler.get_handler(self.id, self.handler, **self.data)


class Config(common.Base):
    """Database table to manage configurations fragments into configurations"""

    __tablename__ = "config"

    id = Column(Integer, primary_key=True)  # Unique association ID
    name = Column(String)  # Name associated this configuration
    assocs_: Iterable = relationship("ConfigAssociation", back_populates="config_")

    def __repr__(self) -> str:
        return f"Config {self.id}: {self.name}"

    def get_sub_handler(self, config_block: str) -> Handler:
        for assoc_ in self.assocs_:
            if assoc_.frag_.tag == config_block:
                return assoc_.frag_.get_handler()
        raise KeyError(f"Could not find config_block {config_block} in {self}")


class ConfigAssociation(common.Base):
    """Database table to associate fragments into configurations"""

    __tablename__ = "config_association"

    id = Column(Integer, primary_key=True)  # Unique association ID
    frag_id = Column(Integer, ForeignKey(Fragment.id))
    config_id = Column(Integer, ForeignKey(Config.id))
    frag_: Fragment = relationship("Fragment", back_populates="assocs_")
    config_: Config = relationship("Config", back_populates="assocs_")
