import os
from typing import Any

import yaml
from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import ScriptBase, TableBase
from lsst.cm.tools.core.rollback import Rollback
from lsst.cm.tools.core.utils import ScriptMethod, StatusEnum, safe_makedirs
from lsst.cm.tools.db.common import CMTable


def write_status_to_yaml(log_url: str, status: StatusEnum) -> None:
    """Write a one line file with just a status flag

    E.g. the file might just contain, `status: completed`
    """
    with open(log_url, "wt", encoding="utf-8") as fout:
        fout.write(f"status: {status.name}\n")


def check_status_from_yaml(log_url: str, current_status: StatusEnum) -> StatusEnum:
    """Read the status from a yaml file

    This just treat the file contents as a dict
    and looks for a field keyed by `status`

    Parameters
    ----------
    log_url : str
        Path to the file in question

    current_status : StatusEnum
        Returned if the file does not exist,
        (i.e., this assumes that the process supposed to make
        the file is still running and the current status still
        applies)

    Returns
    -------
    status : StatusEnum
        The status
    """
    if not os.path.exists(log_url):
        return current_status
    with open(log_url, "rt", encoding="utf-8") as fin:
        fields = yaml.safe_load(fin)
    return StatusEnum[fields["status"]]


def make_butler_associate_command(butler_repo: str, entry: CMTable) -> str:
    """Build and return a butler associate command

    Parameters
    ----------
    butler_repo : str
        The butler repo being used

    entry :
        The database entry we are making the command for

    Returns
    -------
    command : str
        The requested butler command

    Notes
    -----
    This will look for three fields in entry:

    coll_in : str
        This will be the name given to the TAGGED collection

    coll_source : str
        This is the source collection we are pulling from

    data_query : Optional[str]
        A query that can be used to skim out data from the source collection
    """
    coll_in = entry.coll_in
    coll_source = entry.coll_source
    command = f"butler associate {butler_repo} {coll_in} --collections {coll_source}"
    data_query = entry.data_query
    if data_query:
        command += f' --where "{data_query}"'
    return command


def make_butler_chain_command(butler_repo: str, entry: CMTable) -> str:
    """Build and return a butler chain-collection command

    Parameters
    ----------
    butler_repo : str
        The butler repo being used

    entry :
        The database entry we are making the command for

    itr : Iterable
        Iterable with all the source collections

    Returns
    -------
    command : str
        The requested butler command


    Notes
    -----
    This will look for two fields

    entry.coll_out : str
        This will be the name given to the CHAINED collection

    entry.children().coll_out
        These are the source collections
    """
    coll_out = entry.coll_out
    command = f"butler chain-collection {butler_repo} {coll_out}"
    for child in entry.children():
        child_coll = child.coll_out
        command += f" {child_coll}"
    return command


def make_butler_remove_collection_command(butler_repo: str, entry: Any) -> str:
    """Build and return a butler remove-collection command

    Parameters
    ----------
    butler_repo : str
        The butler repo being used

    entry : Any
        The database entry we are making the command for

    Returns
    -------
    command : str
        The requested butler command


    Notes
    -----
    coll_out : str
        This collection will be removed
    """
    coll_out = entry.coll_out
    command = f"butler remove-collection {butler_repo} {coll_out}"
    return command


def make_validate_command(butler_repo: str, entry: Any) -> str:
    """Build and return command to run validation

    Parameters
    ----------
    butler_repo : str
        The butler repo being used

    entry : Any
        The database entry we are making the command for

    Returns
    -------
    command : str
        The requested command

    Notes
    -----
    This is just a placeholder for now
    """
    command = f"validate {butler_repo} --output {entry.coll_validate} {entry.coll_out}"
    return command


def make_bps_command(config_url: str) -> str:
    """Build and return a butler chain-collection command

    Parameters
    ----------
    config_url : str
        The configuration file

    Returns
    -------
    command : str
        The requested command
    """
    return f"bps submit {os.path.abspath(config_url)}"


def write_command_script(script: ScriptBase, command: str, **kwargs: Any) -> None:
    """Write a shell script with a single command

    Parameters
    ----------
    script: ScriptBase

    command: str

    Keywords
    --------
    prepend : str
        Lines added before the command
        E.g., environmental setup or comments

    append : str
        Lines added after the command
        E.g., cleanup

    Returns
    -------
    command : str
        The requested command

    Notes
    -----
    if `script.script_method` == ScriptMethod.bash_stamp
    this will added an echo command to write to the
    stamp file used to check the status of the script
    to the end of the script, i.e., so that the
    stamp file is written when and if the script completes
    """
    prepend = kwargs.get("prepend")
    append = kwargs.get("append")

    safe_makedirs(os.path.dirname(script.script_url))
    with open(script.script_url, "wt", encoding="utf-8") as fout:
        if prepend:
            fout.write(prepend)
        fout.write(command)
        fout.write("\n")
        if append:
            fout.write(append)
        if script.script_method == ScriptMethod.bash_stamp:
            fout.write(f'echo "status: completed" > {os.path.abspath(script.log_url)}\n')
        elif script.script_method == ScriptMethod.bash_callback:  # pragma: no cover
            raise NotImplementedError()


class YamlChecker(Checker):
    """Simple Checker to look in a yaml file for a status flag"""

    def check_url(self, url: str, current_status: StatusEnum) -> StatusEnum:
        return check_status_from_yaml(url, current_status)


class FakeRollback(Rollback):
    """Fakes a command that would remove collections associated to a script"""

    def rollback_script(self, entry: Any, script: TableBase) -> None:
        command = make_butler_remove_collection_command(entry.butler_repo, script)
        print(f"Rolling back {script.db_id}.{script.name} with {command}")
