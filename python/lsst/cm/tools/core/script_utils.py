# This file is part of cm_tools
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os

import yaml
from lsst.cm.tools.core.checker import Checker
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
        command += f" --where {data_query}"
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


class YamlChecker(Checker):
    """Simple Checker to look in a yaml file for a status flag"""
    def check_url(self, url, current_status: StatusEnum) -> StatusEnum:
        """Return the status of the script being checked"""
        return check_status_from_yaml(url, current_status)
