import sys
from typing import Any

import click

from lsst.cm.tools.cli import options
from lsst.cm.tools.core.db_interface import DbInterface

from ..core.handler import Handler
from ..core.utils import LevelEnum, StatusEnum, TableEnum


@click.group()
@click.version_option(package_name="lsst.cm.tools")
def cli() -> None:
    """Campaign management tool"""


@cli.command()
@options.dbi(create=True)
def create(dbi: DbInterface) -> None:
    """Create backing database"""
    assert dbi


@cli.command()
@options.dbi()
@options.level()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.data_query()
@options.butler()
@options.prod_base()
@options.workflow()
@options.config_name()
@options.config_block()
@options.nosubmit()
def insert(
    dbi: DbInterface,
    level: LevelEnum,
    config_name: str,
    config_block: str,
    no_submit: bool,
    **kwargs: Any,
) -> None:
    """Insert a new database entry at a particular level"""
    Handler.no_submit = no_submit
    if level != LevelEnum.production:
        assert config_name is not None
        assert config_block is not None
        the_config = dbi.get_config(config_name)
        the_db_id = dbi.get_db_id(level, **kwargs)
    else:
        the_db_id = None
        the_config = None
    dbi.insert(the_db_id, config_block, the_config, **kwargs)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
def print_tree(dbi: DbInterface, level: LevelEnum, **kwargs: Any) -> None:
    """Print a database table from a given entry in a tree-like format"""
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.print_tree(sys.stdout, level, the_db_id)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
def print(dbi: DbInterface, level: LevelEnum, **kwargs: Any) -> None:  # pylint: disable=redefined-builtin
    """Print a database entry or entries"""
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.print_(sys.stdout, level, the_db_id)


@cli.command()
@options.dbi()
@options.table()
def print_table(dbi: DbInterface, table: TableEnum) -> None:
    """Print a database table"""
    dbi.print_table(sys.stdout, table)


@cli.command()
@options.dbi()
@options.config_name()
def print_config(dbi: DbInterface, config_name: str) -> None:
    """Print a database table from a given entry in a tree-like format"""
    dbi.print_config(sys.stdout, config_name)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
def queue(dbi: DbInterface, level: LevelEnum, **kwargs: Any) -> None:
    """Queue all the ready jobs matching the selection"""
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.queue_jobs(level, the_db_id)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
@options.max_running()
def launch(dbi: DbInterface, level: LevelEnum, no_submit: bool, max_running: int, **kwargs: Any) -> None:
    """Launch all the pending jobs matching the selection"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.launch_jobs(level, the_db_id, max_running)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def check(dbi: DbInterface, level: LevelEnum, no_submit: bool, **kwargs: Any) -> None:
    """Check all database entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.check(level, the_db_id)


@cli.command("accept")
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def accept(dbi: DbInterface, level: LevelEnum, no_submit: bool, **kwargs: Any) -> None:
    """Accept completed entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.accept(level, the_db_id)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def reject(dbi: DbInterface, level: LevelEnum, no_submit: bool, **kwargs: Any) -> None:
    """Reject entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.reject(level, the_db_id)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def supersede(dbi: DbInterface, level: LevelEnum, no_submit: bool, **kwargs: Any) -> None:
    """Mark entries as superseded so that they will be ignored in subsequent
    processing
    """
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.supersede(level, the_db_id)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.status()
@options.nosubmit()
def rollback(dbi: DbInterface, level: LevelEnum, status: StatusEnum, no_submit: bool, **kwargs: Any) -> None:
    """Rollback entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.rollback(level, the_db_id, status)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.status()
@options.max_running()
def fake_run(dbi: DbInterface, level: LevelEnum, status: StatusEnum, **kwargs: Any) -> None:
    """Pretend to run workflows, this is for testing"""
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.fake_run(level, the_db_id, status)


@cli.command()
@options.dbi()
@options.level()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.script()
@options.status()
@options.max_running()
def fake_script(
    dbi: DbInterface, level: LevelEnum, status: StatusEnum, script_name: str, **kwargs: Any
) -> None:
    """Pretend to run workflows, this is for testing"""
    the_db_id = dbi.get_db_id(level, **kwargs)
    dbi.fake_script(level, the_db_id, script_name, status)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.nosubmit()
@options.max_running()
def daemon(dbi: DbInterface, no_submit: bool, max_running: int, **kwargs: Any) -> None:
    """Run a loop"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(LevelEnum.campaign, **kwargs)
    dbi.daemon(the_db_id, max_running)


@cli.command()
@options.dbi()
@options.config_yaml()
@options.config_name()
def parse(dbi: DbInterface, config_yaml: str, config_name: str) -> None:
    """Parse a configuration file"""
    dbi.parse_config(config_name, config_yaml)
