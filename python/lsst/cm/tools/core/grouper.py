from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Optional

from lsst.cm.tools.core.dbid import DbId

if TYPE_CHECKING:  # pragma: no cover
    from lsst.cm.tools.core.db_interface import DbInterface


class Grouper:
    """Base class to build groups for a given processing step

    Derived classes should implement the `_do_call` method
    to return an Iterable which returns a `dict` keyed
    by group name, with values being the configuration parameters
    to insert for the associated group entry.
    """

    def __init__(self) -> None:
        self.config: Optional[dict[str, Any]] = None
        self.dbi: Optional[DbInterface] = None
        self.parent_db_id: Optional[DbId] = None
        self.data = None

    def __call__(
        self, config: dict[str, Any], dbi: DbInterface, parent_db_id: DbId, data: Any, **kwargs: Any
    ) -> Iterable:
        """Return an Iterable over the groups we should make

        Parameters
        ----------
        config : dict[str, Any]
            Any configuration needed to build the groups

        dbi : DbInterface
            Connection to the database

        parent_db_id : DbId
            DdId for the `Step` object that will be the parent to these groups

        data : Any
            Data associated to the parent object

        Keywords
        --------
        Keywords can be used to override the configuration
        """
        self.config = config.copy()
        self.config.update(**kwargs)
        self.dbi = dbi
        self.parent_db_id = parent_db_id
        self.data = data
        return self._do_call()

    def _do_call(self) -> Iterable:
        raise NotImplementedError()
