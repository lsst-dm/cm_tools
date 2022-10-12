from __future__ import annotations

import types
from typing import Any

from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name

from lsst.cm.tools.core.db_interface import ScriptBase


class Checker:
    """Base class to check on script status

    The derived classes should implement the `check_url` method
    with returns a StatusEnum based on querying the URL.

    Typically, this could mean scanning a log file, or checking the existance
    of a file at the URL, or querying a server at that URL.
    """

    checker_cache: dict[str, Checker] = {}

    @staticmethod
    def get_checker(class_name: str) -> Checker:
        """Create and return a status checker

        Parameters
        ----------
        class_name : str
            Name of the Checker class requested

        Returns
        -------
        check : Checker
            Requested Checker

        Notes
        -----
        There is a layer of caching here.
        1.  A `dict` of Checker objects, keyed by class name
        """
        if class_name is None:
            return None
        cached_checker = Checker.checker_cache.get(class_name)
        if cached_checker is None:
            checker_class = doImport(class_name)
            if isinstance(checker_class, types.ModuleType):
                raise TypeError()
            cached_checker = checker_class()
            Checker.checker_cache[class_name] = cached_checker
        return cached_checker

    def get_checker_class_name(self) -> str:
        """Return this class's full name"""
        return get_full_type_name(self)

    def check_url(self, script: ScriptBase) -> dict[str, Any]:
        """Return the status of the script being checked

        Parameters
        ----------
        script: ScriptBase
            Script we are checking the status of

        Returns
        -------
        status : StatusEnum
            Status of the script
        """
        raise NotImplementedError()
