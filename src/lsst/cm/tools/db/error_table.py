import enum
from typing import Iterable

from sqlalchemy import Boolean, Column, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from lsst.cm.tools.db import common
from lsst.cm.tools.db.job import Job


class ErrorType(enum.Enum):
    """ """

    pipelines = 0
    panda = 1
    usdf_is_on_fire = 2


class ErrorAction(enum.Enum):
    """ """

    ignore = 0
    rescue_job = 1
    email_orion = 2
    fail_job = 3
    email_yusra = 4


class ErrorStatus(enum.Enum):
    """ """

    waiting_for_instructions = -1
    ignored = 0
    hold_to_rescue = 1
    rescuing = 2
    rescued = 3
    hold_to_fail = 4
    failed = 5


class ErrorType(common.Base):
    """Database table to keep track of types of errors."""

    __tablename__ = "error_type"

    id = Column(Integer, primary_key=True)  # Unique ID
    panda_err_code = Column(String)
    diagnostic_message = Column(String)
    jira_ticket = Column(String)
    function = Column(String)
    is_known = Column(Boolean)
    is_resolved = Column(Boolean)
    is_rescueable = Column(Boolean)
    error_type = Column(Enum(ErrorType))
    action = Column(Enum(ErrorAction))
    max_intensity = Column(Float)
    instances_: Iterable = relationship("ErrorInstance", back_populates="error_type_")


class ErrorInstance(common.Base):
    """Database table to keep track of individual errors."""

    __tablename__ = "error_instance"

    id = Column(Integer, primary_key=True)  # Unique ID
    job_id = Column(Integer, ForeignKey(Job.id))
    error_type_id = Column(Integer, ForeignKey(ErrorType.id))

    panda_err_code = Column(String)
    diagnostic_message = Column(String)
    function = Column(String)
    log_file_url = Column(String)  # some_file.log:3145
    data_id = Column(String)  # detector=32, visit=1341323412 or tract=1312, filter=really_blue
    error_type = Column(Enum(ErrorType))
    error_status = Column(Enum(ErrorStatus))

    job_: Job = relationship("Job", back_populates="errors_")
    error_type_: ErrorType = relationship("ErrorType", back_populates="instances_")
