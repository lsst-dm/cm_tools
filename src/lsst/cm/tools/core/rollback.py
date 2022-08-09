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
            Requested Rollback

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
            Database entry associated to the script to be rolled-back

        script : TableBase
            Script being rolled back
        """
        raise NotImplementedError()
