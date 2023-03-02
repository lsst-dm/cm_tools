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
    usdf = 2


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
    __allow_unmapped__ = True

    id = Column(Integer, primary_key=True)  # Unique ID
    panda_err_code = Column(String)
    error_name = Column(String, unique=True)  # unique=True prevents loading the same error twice
    diagnostic_message = Column(String)
    jira_ticket = Column(String)
    function = Column(String)
    is_resolved = Column(Boolean)
    is_rescueable = Column(Boolean)
    error_flavor = Column(Enum(ErrorFlavor))
    action = Column(Enum(ErrorAction))
    max_intensity = Column(Float)
    instances_: Iterable = relationship("ErrorInstance", back_populates="error_type_")

    def __repr__(self):
        s = f"Id={self.id}\n"
        s += f"  Name: {self.error_name} Panda Code: {self.panda_err_code}  Function: {self.function}\n"
        s += f"  JIRA: {self.jira_ticket}\n"
        s += "  Flags (known, resolved, rescuable): "
        s += f"{self.is_resolved}, {self.is_rescueable}\n"
        if len(self.diagnostic_message) > 150:
            diag_message = self.diagnostic_message[0:149]
        else:
            diag_message = self.diagnostic_message
        s += f"    {diag_message}"
        return s


class ErrorInstance(common.Base):
    """Database table to keep track of individual errors."""

    __tablename__ = "error_instance"
    __allow_unmapped__ = True

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

    def __repr__(self):
        error_type = self.error_type_
        if error_type is not None:
            is_resolved = error_type.is_resolved
            is_rescueable = error_type.is_rescueable
        else:
            is_resolved = False
            is_rescueable = False

        s = f"Id={self.id} {self.job_id}\n"
        s += f"  Error_name: {self.error_name} {self.error_type_id} Function: {self.function}\n"
        s += f"  {self.panda_err_code}\n"
        s += f"  Data_id: {self.data_id}\n"
        s += "  Flags (known, resolved, rescuable): "
        s += f"{is_resolved}, {is_rescueable}\n"
        if len(self.diagnostic_message) > 150:
            diag_message = self.diagnostic_message[0:150]
        else:
            diag_message = self.diagnostic_message
        s += f"    {diag_message}"
        return s
