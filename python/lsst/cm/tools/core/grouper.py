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

from __future__ import annotations  # Needed for class member returning class

from typing import TYPE_CHECKING, Any, Iterable

from lsst.cm.tools.core.dbid import DbId

if TYPE_CHECKING:  # pragma: no cover
    from lsst.cm.tools.core.db_interface import DbInterface


class Grouper:
    """Base class to build groups for a given processing step"""

    def __init__(self):
        self.config = None
        self.dbi = None
        self.parent_db_id = None
        self.data = None

    def __call__(
        self, config: dict[str, Any], dbi: DbInterface, parent_db_id: DbId, data, **kwargs
    ) -> Iterable:
        self.config = config.copy()
        self.config.update(**kwargs)
        self.dbi = dbi
        self.parent_db_id = parent_db_id
        self.data = data
        return self._do_call()

    def _do_call(self):
        raise NotImplementedError()
