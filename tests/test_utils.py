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

from lsst.cm.tools.core.utils import LevelEnum, StatusEnum


def test_level_enum():
    for key_ in list(LevelEnum.__members__.keys()):
        level = LevelEnum[key_]
        if level == LevelEnum.production:
            assert level.parent() is None
        else:
            assert level.parent().value == level.value - 1
        if level == LevelEnum.workflow:
            assert level.child() is None
        else:
            assert level.child().value == level.value + 1


def test_status_enum():
    for key_ in list(StatusEnum.__members__.keys()):
        status = StatusEnum[key_]
        assert status.ignore() == (status.value < 0)
