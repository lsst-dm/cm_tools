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

from lsst.cm.tools.core.utils import StatusEnum
from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name


class Checker:
    """Base class to check on script status
    """

    checker_cache = {}

    @staticmethod
    def get_checker(class_name: str) -> Checker:
        """Create and return a handler

        Parameters
        ----------
        class_name : str
            Name of the Checker class requested

        Returns
        -------
        check : Checker
            The requested Checker

        Notes
        -----
        There is a layer of caching here.
        1.  A `dict` of HandlerChecker objects, keyed by class name
        it has not changed.
        """
        cached_checker = Checker.checker_cache.get(class_name)
        if cached_checker is None:
            checker_class = doImport(class_name)
            cached_checker = checker_class()  # type: ignore
            Checker.checker_cache[class_name] = cached_checker
        return cached_checker

    def get_checker_class_name(self) -> str:
        """Return this classes full name"""
        return get_full_type_name(self)

    def check_url(self, url, current_status: StatusEnum) -> StatusEnum:
        """Return the status of the script being checked"""
        raise NotImplementedError()
