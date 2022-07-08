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


class StatusEnum(enum.Enum):
    failed = -3  # Processing failed
    rejected = -2  # Marked as rejected
    superseded = -1  # Marked as superseded
    waiting = 0  # Inputs are not ready
    ready = 1  # Inputs are ready
    pending = 2  # Jobs are queued for submission
    running = 3  # Jobs are running
    part_fail = 4  # Partially failed, could be accepted
    completed = 5  # Completed, awaiting review
    accepted = 6  # Completed, reviewed and accepted

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
