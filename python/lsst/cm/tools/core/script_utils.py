import os

import yaml
from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase
from lsst.cm.tools.core.utils import StatusEnum


def write_status_to_yaml(log_url, status: StatusEnum) -> None:
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


def make_butler_associate_command(butler_repo: str, data) -> str:
    """Build and return a butler associate command

    Parameters
    ----------
    butler_repo : str
        The butler repo being used

    data :
        The database entry we are making the command for

    Returns
    -------
    command : str
        The requested butler command


    Notes
    -----
    This will look for three fields in data:

    coll_in : str
        This will be the name given to the TAGGED collection

    coll_source : str
        This is the source collection we are pulling from

    data_query : Optional[str]
        A query that can be used to skim out data from the source collection
    """
    coll_in = data.coll_in
    coll_source = data.coll_source
    command = f"butler associate {butler_repo} {coll_in} --collections {coll_source}"
    data_query = data.data_query
    if data_query:
        command += f' --where "{data_query}"'
    return command


def make_butler_chain_command(butler_repo: str, data, itr) -> str:
    """Build and return a butler chain-collection command

    Parameters
    ----------
    butler_repo : str
        The butler repo being used

    data :
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

    data.coll_out : str
        This will be the name given to the CHAINED collection

    itr.coll_out
        These are the source collections
    """
    coll_out = data.coll_out
    command = f"butler chain-collection {butler_repo} {coll_out}"
    for child in itr:
        child_coll = child.coll_out
        command += f" {child_coll}"
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


class YamlChecker(Checker):
    """Simple Checker to look in a yaml file for a status flag"""

    def check_url(self, url, current_status: StatusEnum) -> StatusEnum:
        """Return the status of the script being checked"""
        return check_status_from_yaml(url, current_status)


def add_command_script(dbi: DbInterface, command, script_data, mode, **kwargs) -> ScriptBase:
    script = dbi.add_script(checker=kwargs.get("checker"), **script_data)
    prepend = kwargs.get("prepend")
    append = kwargs.get("append")
    with open(script.script_url, "wt", encoding="utf-8") as fout:
        if prepend:
            fout.write(prepend)
        fout.write(command)
        fout.write("\n")
        if append:
            fout.write(append)
        if mode == "callback_stamp":
            fout.write(f'echo "status: completed" > {os.path.abspath(script.log_url)}\n')
        elif mode == "callback_cm":
            fout.write(f"cm set_script_status --script {script.id} --status completed\n")

    if kwargs.get("fake_stamp"):
        write_status_to_yaml(script.log_url, StatusEnum.completed)
    if kwargs.get("fake_callback"):
        pass
    return script
