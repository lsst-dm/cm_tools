import sys
from typing import Any, Tuple

import click

from lsst.cm.tools.cli import options
from lsst.cm.tools.core.db_interface import DbInterface

from ..core.checker import Checker
from ..core.handler import Handler
from ..core.panda_utils import PandaChecker, print_errors_aggregate
from ..core.utils import LevelEnum, ScriptMethod, StatusEnum, TableEnum


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
@options.lsst_version()
@options.root_coll()
@options.prod_base()
@options.workflow()
@options.config_name()
@options.config_block()
@options.script_method()
def insert(
    dbi: DbInterface,
    config_name: str,
    config_block: str,
    script_method: ScriptMethod,
    **kwargs: Any,
) -> None:
    """Insert a new database entry at a particular level"""
    Handler.script_method = script_method
    fullname = kwargs.pop("fullname")
    if fullname is not None:
        names = dbi.parse_fullname(fullname)
        kwargs.update(**names)
        the_db_id = dbi.get_db_id(**names)
    else:
        the_db_id = dbi.get_db_id(**kwargs)
    if the_db_id.level() is not None:
        assert config_name is not None
        assert config_block is not None
        the_config = dbi.get_config(config_name)
    else:
        the_config = None
    dbi.insert(the_db_id, config_block, the_config, **kwargs)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.config_block()
@options.script_method()
def insert_step(
    dbi: DbInterface,
    config_block: str,
    script_method: ScriptMethod,
    **kwargs: Any,
) -> None:
    """Insert a new step into an existing campaign"""
    Handler.script_method = script_method
    fullname = kwargs.pop("fullname")
    if fullname is not None:
        names = dbi.parse_fullname(fullname)
        kwargs.update(**names)
        the_db_id = dbi.get_db_id(**names)
    else:
        the_db_id = dbi.get_db_id(**kwargs)

    assert the_db_id.level() == LevelEnum.campaign
    assert config_block is not None
    dbi.insert_step(the_db_id, config_block, **kwargs)


@cli.command()
@options.dbi()
@options.fullname()
@options.config_block()
@options.script_method()
def insert_rescue(
    dbi: DbInterface,
    config_block: str,
    script_method: ScriptMethod,
    **kwargs: Any,
) -> None:
    """Insert a rescue workflow"""
    Handler.script_method = script_method
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.insert_rescue(the_db_id, config_block, **kwargs)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
def summarize_output(
    dbi: DbInterface,
    **kwargs: Any,
) -> None:
    """Summarize the output of a particular entry"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.summarize_output(sys.stdout, the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
def associate_kludge(
    dbi: DbInterface,
    **kwargs: Any,
) -> None:
    """Kludge the butler associate command"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.associate_kludge(the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.config_name()
@options.config_block()
@options.script_method()
def add_script(
    dbi: DbInterface,
    config_name: str,
    config_block: str,
    script_method: ScriptMethod,
    **kwargs: Any,
) -> None:
    """Insert a new script associated to a particular entry"""
    Handler.script_method = script_method
    the_db_id = dbi.get_db_id(**kwargs)
    the_config = dbi.get_config(config_name)
    dbi.add_script(the_db_id, config_block, the_config, **kwargs)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.config_name()
@options.config_block()
@options.script_method()
def add_job(
    dbi: DbInterface,
    config_name: str,
    config_block: str,
    script_method: ScriptMethod,
    **kwargs: Any,
) -> None:
    """Insert a new batch job associated to a particular entry"""
    Handler.script_method = script_method
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
@options.fmt()
def print(dbi: DbInterface, **kwargs: Any) -> None:  # pylint: disable=redefined-builtin
    """Print a database entry or entries"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.print_(sys.stdout, the_db_id.level(), the_db_id, fmt=kwargs.get("fmt"))


@cli.command()
@options.dbi()
@options.table()
@options.fmt()
def print_table(dbi: DbInterface, table: TableEnum, **kwargs: Any) -> None:
    """Print a database table"""
    dbi.print_table(sys.stdout, table, **kwargs)


@cli.command()
@options.dbi()
@options.config_name()
def print_config(dbi: DbInterface, config_name: str) -> None:
    """Print a information about a configuration"""
    dbi.print_config(sys.stdout, config_name)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.script_method()
def queue(dbi: DbInterface, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Queue all the prepared jobs matching the selection"""
    Handler.script_method = script_method
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
@options.script_method()
def requeue(dbi: DbInterface, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Queue all the prepared jobs matching the selection"""
    Handler.script_method = script_method
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.requeue_jobs(the_db_id.level(), the_db_id)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.script()
@options.script_method()
def rerun_scripts(dbi: DbInterface, script_name: str, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Rerun particular failed  scripts"""
    Handler.script_method = script_method
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.rerun_scripts(the_db_id.level(), the_db_id, script_name)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.script_method()
@options.max_running()
def launch(dbi: DbInterface, script_method: ScriptMethod, max_running: int, **kwargs: Any) -> None:
    """Launch all the pending jobs matching the selection"""
    Handler.script_method = script_method
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
@options.script_method()
@options.username()
def check(dbi: DbInterface, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Check all the matching database entries"""
    Handler.script_method = script_method
    Checker.generic_username = kwargs.get("username", "None")
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
@options.script_method()
@options.rescuable()
def accept(dbi: DbInterface, script_method: ScriptMethod, rescuable: bool, **kwargs: Any) -> None:
    """Accept all the completed matching entries"""
    Handler.script_method = script_method
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.accept(the_db_id.level(), the_db_id, rescuable)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.script_method()
def reject(dbi: DbInterface, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Reject all the matching entries"""
    Handler.script_method = script_method
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
@options.script_method()
@options.purge()
def supersede(dbi: DbInterface, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Mark entries as superseded so that they will be ignored in subsequent
    processing
    """
    Handler.script_method = script_method
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.supersede(the_db_id.level(), the_db_id, kwargs.get("purge", False))


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.status()
@options.script_method()
def rollback(dbi: DbInterface, status: StatusEnum, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Rollback all the matching entries to a given status"""
    Handler.script_method = script_method
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
@options.script_method()
def fake_run(dbi: DbInterface, status: StatusEnum, script_method: ScriptMethod, **kwargs: Any) -> None:
    """Pretend to run workflows, this is for testing"""
    Handler.script_method = script_method
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
@options.script_method()
def fake_script(dbi: DbInterface, status: StatusEnum, script_name: str, **kwargs: Any) -> None:
    """Pretend to run scripts, this is for testing"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.fake_script(the_db_id.level(), the_db_id, script_name, status)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.step()
@options.group()
@options.workflow()
@options.status()
def set_status(dbi: DbInterface, status: StatusEnum, **kwargs: Any) -> None:
    """Explicitly set the status of an entry"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.set_status(the_db_id.level(), the_db_id, status)


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
@options.script_method()
@options.idx()
def set_job_status(dbi: DbInterface, status: StatusEnum, script_name: str, idx: int, **kwargs: Any) -> None:
    """Explicitly set the status of a particular job"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.set_job_status(the_db_id.level(), the_db_id, script_name, idx, status)


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
@options.script_method()
@options.idx()
def set_script_status(
    dbi: DbInterface, status: StatusEnum, script_name: str, idx: int, **kwargs: Any
) -> None:
    """Explicitly set the status of a particular script"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.set_script_status(the_db_id.level(), the_db_id, script_name, idx, status)


@cli.command()
@options.dbi()
@options.fullname()
@options.production()
@options.campaign()
@options.script_method()
@options.max_running()
@options.sleep_time()
@options.n_iter()
@options.verbose()
@options.log_file()
def daemon(
    dbi: DbInterface,
    script_method: ScriptMethod,
    max_running: int,
    sleep_time: int,
    n_iter: int,
    verbose: bool,
    log_file: Any,
    **kwargs: Any,
) -> None:
    """Run a processing loop"""
    Handler.script_method = script_method
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.daemon(the_db_id, max_running, sleep_time, n_iter, verbose, log_file)


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
def load_error_types(dbi: DbInterface, config_yaml: str) -> None:
    """Load error types from a configuration file"""
    dbi.load_error_types(config_yaml)


@cli.command()
@options.dbi()
@options.error_name()
@options.update_item()
def modify_error_type(dbi: DbInterface, error_name: str, update_item: Tuple[str, str]) -> None:
    """Modify values of a particular ErrorType"""
    kwargs = {update_item[0]: update_item[1]}
    dbi.modify_error_type(error_name, **kwargs)


@cli.command()
@options.dbi()
@options.error_name()
def report_error_trend(dbi: DbInterface, error_name: str) -> None:
    """Report if errors have been seen in prior workflows and if so, when"""
    dbi.report_error_trend(sys.stdout, error_name)


@cli.command()
@options.dbi()
@options.panda_code()
@options.diag_message()
def match_error_type(dbi: DbInterface, panda_code: str, diag_message: str) -> None:
    """Search for a specific error based on the error messages"""
    error_type = dbi.match_error_type(panda_code, diag_message)
    sys.stdout.write(f"{repr(error_type)}\n")


@cli.command()
@options.dbi()
def rematch_errors(dbi: DbInterface) -> None:
    """Match errors against existing error types"""
    dbi.rematch_errors()


@cli.command()
@options.dbi()
@options.config_yaml()
@options.error_yaml()
def match_file_errors(dbi: DbInterface, config_yaml: str, error_yaml: str) -> None:
    """Match errors against existing error types in a file"""
    dbi.match_file_errors(config_yaml, error_yaml)


@cli.command()
@options.dbi()
@options.config_yaml()
@options.config_name()
def extend(dbi: DbInterface, config_yaml: str, config_name: str) -> None:
    """Parse a configuration file and add the fragments to another config"""
    dbi.extend_config(config_name, config_yaml)


@cli.command()
@options.dbi()
@options.panda_url(type=int)
@options.username()
def check_panda_job(dbi: DbInterface, panda_url: int, username: str) -> list[str]:
    """Check the status of a panda job"""
    pc = PandaChecker()
    status, errors_aggregate = pc.check_panda_status(dbi, panda_url, username)
    errors_aggregate["panda_status"] = status
    print_errors_aggregate(sys.stdout, errors_aggregate)


@cli.command()
@options.dbi()
@options.panda_url(type=int)
@options.username()
def get_panda_errors(dbi: DbInterface, panda_url: int, username: str):
    """Check the status of a finished panda task"""
    pc = PandaChecker()
    errors_aggregate, _, _ = pc.get_panda_errors(dbi, panda_url, username)
    print_errors_aggregate(sys.stdout, errors_aggregate)


@cli.command()
@options.dbi()
@options.fullname()
@options.summary()
@options.review()
@options.yaml_output()
def report_errors(
    dbi: DbInterface,
    **kwargs: Any,
) -> None:
    """Summarize the output of a particular entry"""
    the_db_id = dbi.get_db_id(**kwargs)
    dbi.report_errors(sys.stdout, the_db_id.level(), the_db_id, **kwargs)
