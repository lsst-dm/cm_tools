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

from lsst.cm.tools.core.db_interface import ScriptBase
from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name


class Rollback:
    """Base class to rollback scripts

    The derived classes should implement the `rollback` method
    with returns a StatusEnum based on querying the URL.

    Typically, this could mean scanning a log file, or checking the existance
    of a file at the URL, or querying a server at that URL.
    """

    rollback_cache: dict[str, Rollback] = {}

    @staticmethod
    def get_rollback(class_name: str) -> Rollback:
        """Create and return a handler

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

    def rollback_script(self, script: ScriptBase) -> None:
        """Rollback the script in question"""
        raise NotImplementedError()


class FakeRollback(Rollback):
    def rollback_script(self, script: ScriptBase) -> None:
        """Rollback the script in question"""
        print(f"Rolling back {script.db_id}.{script.name}")
