from typing import Any

from sqlalchemy import Table, create_engine
from sqlalchemy_utils import create_database, database_exists

from lsst.cm.tools.core.utils import LevelEnum, TableEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.dependency import Dependency
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.job import Job
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.script import Script
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow import Workflow


def create_db(engine: Any) -> None:
    """Creates a database as specific by `engine.url`

    Populates the database with empty tables
    """
    create_database(engine.url)
    common.Base.metadata.create_all(engine)


def build_engine(db_url: str, **kwargs: Any) -> Any:
    """Return the sqlalchemy engine, building the database if needed"""
    kwcopy = kwargs.copy()
    create = kwcopy.pop("create", False)
    engine = create_engine(db_url, **kwcopy)
    if not database_exists(engine.url):
        if create:
            create_db(engine)
    if not database_exists(engine.url):
        raise RuntimeError(f"Failed to access or create database {db_url}")
    return engine


def get_table_for_level(level: LevelEnum) -> Table:
    """Return the Table corresponding to a `level`"""
    all_tables = {
        LevelEnum.production: Production,
        LevelEnum.campaign: Campaign,
        LevelEnum.step: Step,
        LevelEnum.group: Group,
        LevelEnum.workflow: Workflow,
    }
    return all_tables[level]


def get_table(which_table: TableEnum) -> Table:
    """Return the Table corresponding to a `level`"""
    all_tables = {
        TableEnum.production: Production,
        TableEnum.campaign: Campaign,
        TableEnum.step: Step,
        TableEnum.group: Group,
        TableEnum.workflow: Workflow,
        TableEnum.script: Script,
        TableEnum.job: Job,
        TableEnum.dependency: Dependency,
    }
    return all_tables[which_table]
