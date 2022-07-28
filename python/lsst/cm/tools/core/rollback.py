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

from typing import TYPE_CHECKING, Any

from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name

if TYPE_CHECKING:  # pragma: no cover
    from lsst.cm.tools.core.db_interface import TableBase


class Rollback:
    """Base class to rollback scripts

    The derived classes should implement the `rollback_script`
    which cleans up any output of the rolled-back script

    Typically this would mean removing the output collection
    """

    rollback_cache: dict[str, Rollback] = {}

    @staticmethod
    def get_rollback(class_name: str) -> Rollback:
        """Create and return a Rollback handler

        Parameters
        ----------
        class_name : str
            Name of the Rollback class requested

        Returns
        -------
        rollback : Rollback
            The requested Rollback

        Notes
        -----
        There is a layer of caching here.
        1.  A `dict` of Rollback objects, keyed by class name
        """
        cached_rollback = Rollback.rollback_cache.get(class_name)
        if cached_rollback is None:
            rollback_class = doImport(class_name)
            cached_rollback = rollback_class()  # type: ignore
            Rollback.rollback_cache[class_name] = cached_rollback
        return cached_rollback

    def get_rollback_class_name(self) -> str:
        """Return this class's full name"""
        return get_full_type_name(self)

    def rollback_script(self, entry: Any, script: TableBase) -> None:
        """Rollback the script in question

        Parameters
        ----------
        entry : Any
            The database entry associated to the script be rolled-back

        script : TableBase
            The script or workflow being rolled back
        """
        raise NotImplementedError()
