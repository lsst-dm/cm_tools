from __future__ import annotations

import types

from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name

from lsst.cm.tools.core.utils import StatusEnum


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

    def check_url(self, url: str, current_status: StatusEnum) -> StatusEnum:
        """Return the status of the script being checked

        Parameters
        ----------
        url : str
            URL used to check the script status

        current_status : StatusEnum
            Can be used as output if the URL is empty
            I.e., the script hasn't generated it yet

        Returns
        -------
        status : StatusEnum
            Status of the script
        """
        raise NotImplementedError()
