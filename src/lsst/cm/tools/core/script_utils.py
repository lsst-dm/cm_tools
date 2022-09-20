import os
from typing import Any, Optional

import yaml

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import CMTableBase, ScriptBase, TableBase
from lsst.cm.tools.core.rollback import Rollback
from lsst.cm.tools.core.utils import StatusEnum, safe_makedirs


def write_status_to_yaml(stamp_url: str, status: StatusEnum) -> None:
    """Write a one line file with just a status flag

    E.g. the file might just contain, `status: completed`
    """
    with open(stamp_url, "wt", encoding="utf-8") as fout:
        fout.write(f"status: {status.name}\n")


def check_status_from_yaml(stamp_url: str, current_status: StatusEnum) -> StatusEnum:
    """Read the status from a yaml file

    This just treat the file contents as a dict
    and looks for a field keyed by `status`

    Parameters
    ----------
    stamp_url : str
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
    if not os.path.exists(stamp_url):
        return current_status
    with open(stamp_url, "rt", encoding="utf-8") as fin:
        fields = yaml.safe_load(fin)
    return StatusEnum[fields["status"]]


def make_butler_associate_command(
    butler_repo: str,
    coll_in: str,
    coll_source: str,
    data_query: Optional[str],
) -> str:
    """Build and return a butler associate command

    Parameters
    ----------
    butler_repo : str
        Butler repo being used

    coll_in : str
        This will be the name given to the TAGGED collection

    coll_source : str
        This is the source collection we are pulling from

    data_query : Optional[str]
        Query that can be used to skim out data from the source collection

    Returns
    -------
    command : str
        Requested butler command

    """
    command = f"butler associate {butler_repo} {coll_in} --collections {coll_source}"
    if data_query:
        command += f' --where "{data_query}"'
    return command


def make_butler_chain_command(butler_repo: str, coll_out: str, input_colls: list[str]) -> str:
    """Build and return a butler collection-chain command

    Parameters
    ----------
    butler_repo : str
        Butler repo being used

    coll_out : str
        This will be the name given to the CHAINED collection

    input_colls : list[str]
        These are the source collections

    Returns
    -------
    command : str
        Requested butler command
    """
    command = f"butler collection-chain {butler_repo} {coll_out}"
    for input_coll in input_colls:
        command += f" {input_coll}"
    return command


def make_butler_remove_collection_command(butler_repo: str, coll_out: str) -> str:
    """Build and return a butler remove-collection command

    Parameters
    ----------
    butler_repo : str
        Butler repo being used

    coll_out : str
        This collection will be removed

    Returns
    -------
    command : str
        Requested butler command
    """
    command = f"butler remove-collection {butler_repo} {coll_out}"
    return command


def make_validate_command(butler_repo: str, coll_validate: str, coll_out: str) -> str:
    """Build and return command to run validation

    Parameters
    ----------
    butler_repo : str
        Butler repo being used

    coll_validate : str
        Where to write the validation

    coll_out : str
        The collection being validated

    Returns
    -------
    command : str
        Requested command
    """
    command = f"validate {butler_repo} --output {coll_validate} {coll_out}"
    return command


def make_bps_command(config_url: str) -> str:
    """Build and return command to submit a bps job

    Parameters
    ----------
    config_url : str
        Configuration file

    Returns
    -------
    command : str
        Requested command
    """
    log_url = config_url.replace(".yaml", ".json")
    stamp_url = config_url.replace(".yaml", ".stamp")
    return f"bps --log-file {log_url} --no-log-tty submit {os.path.abspath(config_url)} > {stamp_url}"


def write_command_script(script: ScriptBase, command: str, **kwargs: Any) -> None:
    """Write a shell script with a single command

    This wraps the commond in some tooling needed
    by the CM tooling

    Parameters
    ----------
    script: ScriptBase
        Database entry we are writing script for

    command: str
        Command to run

    Keywords
    --------
    prepend : str
        Lines added before the command
        E.g., environmental setup or comments

    append : str
        Lines added after the command
        E.g., cleanup

    stamp : StatusEnum
        This will added an echo command to write to the
        stamp file used to check the status of the script
        to the end of the script, i.e., so that the
        stamp file is written when and if the script completes

    callback : StatusEnum
        This will add a callback to cm to step the script status

    fake : bool
        This will only echo the command, no actually run int
    """
    prepend = kwargs.get("prepend")
    append = kwargs.get("append")
    stamp = kwargs.get("stamp")
    callback = kwargs.get("callback")
    fake = kwargs.get("fake")

    safe_makedirs(os.path.dirname(script.script_url))
    with open(script.script_url, "wt", encoding="utf-8") as fout:
        if prepend:
            fout.write(f"{prepend}\n")
        if fake:
            command = f"echo '{command}'"
        fout.write(command)
        fout.write("\n")
        if append:
            fout.write(f"{append}\n")
        if stamp:
            fout.write(f'echo "status: {stamp}" > {os.path.abspath(script.stamp_url)}\n')
        if callback:  # pragma: no cover
            raise NotImplementedError()


class YamlChecker(Checker):
    """Simple Checker to look in a yaml file for a status flag"""

    def check_url(self, url: str, current_status: StatusEnum) -> StatusEnum:
        return check_status_from_yaml(url, current_status)


class FakeRollback(Rollback):
    """Fakes a command that would remove collections associated to a script"""

    def rollback_script(self, entry: CMTableBase, script: TableBase) -> None:
        command = make_butler_remove_collection_command(entry.butler_repo, script.coll_out)
        print(f"Rolling back {script.db_id}.{script.name} with {command}")
