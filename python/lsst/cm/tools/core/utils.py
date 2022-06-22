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
    waiting = 0
    ready = 1
    pending = 2
    running = 3
    failed = 4
    done = 5
    superseeded = 6


class LevelEnum(enum.Enum):
    production = 0
    campaign = 1
    step = 2
    group = 3
    workflow = 4


def level_name(level: LevelEnum):
    all_levels = {
        LevelEnum.production: 'production',
        LevelEnum.campaign: 'campaign',
        LevelEnum.step: 'step',
        LevelEnum.group: 'group',
        LevelEnum.workflow: 'workflow'}
    return all_levels[level]
