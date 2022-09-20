import sys
from typing import Any

import click

from lsst.cm.tools.cli import options
from lsst.cm.tools.core.db_interface import DbInterface

from ..core.handler import Handler
from ..core.utils import StatusEnum, TableEnum


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
@options.fullname()
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
    config_name: str,
    config_block: str,
    no_submit: bool,
    **kwargs: Any,
) -> None:
    """Insert a new database entry at a particular level"""
    Handler.no_submit = no_submit
    fullname = kwargs.pop("fullname")
    if fullname is not None:
        names = dbi.parse_fullname(fullname)
    the_db_id = dbi.get_db_id(**names)
    if the_db_id.level() is not None:
        assert config_name is not None
        assert config_block is not None
        the_config = dbi.get_config(config_name)
    else:
        the_config = None
    kwargs.update(**names)
    dbi.insert(the_db_id, config_block, the_config, **kwargs)


@cli.command()
@options.dbi()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.config_name()
@options.config_block()
@options.nosubmit()
def add_script(
    dbi: DbInterface,
    config_name: str,
    config_block: str,
    no_submit: bool,
    **kwargs: Any,
) -> None:
    """Insert a new database entry at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    the_config = dbi.get_config(config_name)
    dbi.add_script(the_db_id, config_block, the_config, **kwargs)


@cli.command()
@options.dbi()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.config_name()
@options.config_block()
@options.nosubmit()
def add_job(
    dbi: DbInterface,
    config_name: str,
    config_block: str,
    no_submit: bool,
    **kwargs: Any,
) -> None:
    """Insert a new database entry at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    the_config = dbi.get_config(config_name)
    dbi.add_job(the_db_id, config_block, the_config, **kwargs)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
def print_tree(dbi: DbInterface, **kwargs: Any) -> None:
    """Print a database table from a given entry in a tree-like format"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.print_tree(sys.stdout, the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
def print(dbi: DbInterface, **kwargs: Any) -> None:  # pylint: disable=redefined-builtin
    """Print a database entry or entries"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.print_(sys.stdout, the_db_id.level(), the_db_id)


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
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
def queue(dbi: DbInterface, **kwargs: Any) -> None:
    """Queue all the ready jobs matching the selection"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.queue_jobs(the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
@options.max_running()
def launch(dbi: DbInterface, no_submit: bool, max_running: int, **kwargs: Any) -> None:
    """Launch all the pending jobs matching the selection"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.launch_jobs(the_db_id.level(), the_db_id, max_running)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def check(dbi: DbInterface, no_submit: bool, **kwargs: Any) -> None:
    """Check all database entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.check(the_db_id.level(), the_db_id)


@cli.command("accept")
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def accept(dbi: DbInterface, no_submit: bool, **kwargs: Any) -> None:
    """Accept completed entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.accept(the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def reject(dbi: DbInterface, no_submit: bool, **kwargs: Any) -> None:
    """Reject entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.reject(the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.nosubmit()
def supersede(dbi: DbInterface, no_submit: bool, **kwargs: Any) -> None:
    """Mark entries as superseded so that they will be ignored in subsequent
    processing
    """
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.supersede(the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.status()
@options.nosubmit()
def rollback(dbi: DbInterface, status: StatusEnum, no_submit: bool, **kwargs: Any) -> None:
    """Rollback entries at a particular level"""
    Handler.no_submit = no_submit
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.rollback(the_db_id.level(), the_db_id, status)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.status()
@options.max_running()
def fake_run(dbi: DbInterface, status: StatusEnum, **kwargs: Any) -> None:
    """Pretend to run workflows, this is for testing"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.fake_run(the_db_id.level(), the_db_id, status)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.script()
@options.status()
@options.max_running()
def fake_script(dbi: DbInterface, status: StatusEnum, script_name: str, **kwargs: Any) -> None:
    """Pretend to run workflows, this is for testing"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.fake_script(the_db_id.level(), the_db_id, script_name, status)


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
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.daemon(the_db_id, max_running)


@cli.command()
@options.dbi()
@options.config_yaml()
@options.config_name()
def parse(dbi: DbInterface, config_yaml: str, config_name: str) -> None:
    """Parse a configuration file"""
    dbi.parse_config(config_name, config_yaml)


@cli.command()
@options.dbi()
@options.config_yaml()
@options.config_name()
def extend(dbi: DbInterface, config_yaml: str, config_name: str) -> None:
    """Parse a configuration file"""
    dbi.extend_config(config_name, config_yaml)
