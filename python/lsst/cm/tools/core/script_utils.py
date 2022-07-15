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
    """Write a one line file with just a status flag"""
    with open(log_url, "wt", encoding="utf-8") as fout:
        fout.write(f"status: {status.name}\n")


def check_status_from_yaml(log_url, current_status: StatusEnum) -> StatusEnum:
    """Read the status from a yaml file"""
    if not os.path.exists(log_url):
        return current_status
    with open(log_url, "rt", encoding="utf-8") as fin:
        fields = yaml.safe_load(fin)
    return StatusEnum[fields["status"]]


def make_butler_associate_command(butler_repo: str, data) -> str:
    """Build and return a butler associate command"""
    coll_in = data["coll_in"]
    coll_source = data["coll_source"]
    command = f"butler associate {butler_repo} {coll_in} --collections {coll_source}"
    data_query = data["data_query"]
    if data_query:
        command += f" --where {data_query}"
    return command


def make_butler_chain_command(butler_repo: str, data, itr) -> str:
    coll_out = data["coll_out"]
    command = f"butler chain-collection {butler_repo} {coll_out}"
    for child in itr:
        child_coll = child["coll_out"]
        command += f" {child_coll}"
    return command


class YamlChecker(Checker):
    def check_url(self, url, current_status: StatusEnum) -> StatusEnum:
        """Return the status of the script being checked"""
        return check_status_from_yaml(url, current_status)
