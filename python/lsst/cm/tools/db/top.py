from typing import Any, Optional

from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow import Workflow
from sqlalchemy import Table, create_engine


def create_db(engine) -> None:
    """Creates a database as specific by `engine.url`

    Populates the database with empty tables
    """
    from sqlalchemy_utils import create_database  # pylint: disable=import-outside-toplevel

    create_database(engine.url)
    common.Base.metadata.create_all(engine)


def build_engine(db_url, **kwargs: Any):
    """Return the sqlalchemy engine, building the database if needed"""
    from sqlalchemy_utils import database_exists

    kwcopy = kwargs.copy()
    create = kwcopy.pop("create", False)
    engine = create_engine(db_url, **kwcopy)
    if not database_exists(engine.url):
        if create:
            create_db(engine)
    if not database_exists(engine.url):
        raise RuntimeError(f"Failed to access or create database {db_url}")
    return engine


def get_table(level: LevelEnum) -> Table:
    """Return the Table corresponding to a `level`"""
    all_tables = {
        LevelEnum.production: Production,
        LevelEnum.campaign: Campaign,
        LevelEnum.step: Step,
        LevelEnum.group: Group,
        LevelEnum.workflow: Workflow,
    }
    return all_tables[level]


def get_count_query(level: LevelEnum, db_id: Optional[DbId]):
    """Return the query to count rows matching an id"""
    table = get_table(level)
    return table.get_count_query(db_id)


def get_row_query(level: LevelEnum, db_id: DbId, columns=None):
    """Returns the selection a single row given db_id"""
    table = get_table(level)
    return table.get_row_query(db_id, columns=columns)


def get_rows_with_status_query(level: LevelEnum, status: StatusEnum):
    """Returns the selection for all rows with a particular status"""
    table = get_table(level)
    return table.get_rows_with_status_query(status)


def get_id_match_query(level: LevelEnum, parent_id: Optional[int], match_name: Any):
    """Returns the selection to match a particular ID"""
    table = get_table(level)
    return table.get_id_match_query(parent_id=parent_id, match_name=match_name)


def get_match_query(level: LevelEnum, db_id: DbId):
    """Returns the selection all rows given db_id at a given level"""
    table = get_table(level)
    return table.get_match_query(db_id)
