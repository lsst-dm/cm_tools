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

import enum
import os

import yaml


class StatusEnum(enum.Enum):
    failed = -2  # Processing failed
    rejected = -1  # Marked as rejected
    waiting = 0  # Inputs are not ready
    preparing = 1  # Inputs are being prepared
    ready = 2  # Inputs are ready
    pending = 3  # Jobs are queued for submission
    running = 4  # Jobs are running
    collecting = 5  # Jobs have finshed running, collecting results
    completed = 6  # Completed, awaiting review
    accepted = 7  # Completed, reviewed and accepted
    superseded = 8  # Marked as superseded

    def ignore(self):
        return self.value < 0


class LevelEnum(enum.Enum):
    production = 0  # A family of related campaigns
    campaign = 1  # A full data processing campaign
    step = 2  # Part of a campaign that is finished before moving on
    group = 3  # A subset of data that can be processed in paralllel as part of a step
    workflow = 4  # A single Panda workflow

    def parent(self):
        if self.value == 0:
            return None
        return LevelEnum(self.value - 1)

    def child(self):
        if self.value == 4:
            return None
        return LevelEnum(self.value + 1)


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
