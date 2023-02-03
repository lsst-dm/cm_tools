import enum
from typing import Iterable

from sqlalchemy import Boolean, Column, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from lsst.cm.tools.db import common
from lsst.cm.tools.db.job import Job


class ErrorFlavor(enum.Enum):
    """What sort of error are we talking about"""

    pipelines = 0
    panda = 1
    usdf_is_on_fire = 2


class ErrorAction(enum.Enum):
    """What should we do about it?"""

    ignore = 0
    rescue_job = 1
    email_orion = 2
    fail_job = 3
    email_yusra = 4


class ErrorType(common.Base):
    """Database table to keep track of types of errors."""

    __tablename__ = "error_type"

    id = Column(Integer, primary_key=True)  # Unique ID
    panda_err_code = Column(String)
    error_name = Column(String)
    diagnostic_message = Column(String)
    jira_ticket = Column(String)
    function = Column(String)
    is_known = Column(Boolean)
    is_resolved = Column(Boolean)
    is_rescueable = Column(Boolean)
    error_flavor = Column(Enum(ErrorFlavor))
    action = Column(Enum(ErrorAction))
    max_intensity = Column(Float)
    instances_: Iterable = relationship("ErrorInstance", back_populates="error_type_")


class ErrorInstance(common.Base):
    """Database table to keep track of individual errors."""

    __tablename__ = "error_instance"

    id = Column(Integer, primary_key=True)  # Unique ID
    job_id = Column(Integer, ForeignKey(Job.id))
    error_type_id = Column(Integer, ForeignKey(ErrorType.id))
    error_name = Column(String)

    panda_err_code = Column(String)
    diagnostic_message = Column(String)
    function = Column(String)
    log_file_url = Column(String)  # some_file.log:3145
    data_id = Column(String)  # detector=32, visit=1341323412 or tract=1312, filter=really_blue
    error_flavor = Column(Enum(ErrorFlavor))

    job_: Job = relationship("Job", back_populates="errors_")
    error_type_: ErrorType = relationship("ErrorType", back_populates="instances_")
