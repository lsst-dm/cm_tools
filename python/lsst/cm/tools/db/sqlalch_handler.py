from collections import OrderedDict
from typing import Iterable

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum

# import datetime


class SQLAlchemyHandler(Handler):  # noqa
    """SQLAlchemy based Handler

    This contains the implementation details that
    are specific to the SQLAlchemy based DB struture.
    """

    default_config = {}

    step_dict: OrderedDict[str, type] = OrderedDict([])

    def _group_iterator(self, dbi: DbInterface, parent_data_id: DbId, data, **kwargs) -> Iterable:
        step_name = str(kwargs.get("step_name"))
        try:
            grouper_class = self.step_dict[step_name]
            grouper = grouper_class()
        except KeyError as msg:  # pragma: no cover
            raise KeyError(f"No Grouper object associated to step {step_name}") from msg
        return grouper(self.config, dbi, parent_data_id, data, **kwargs)

    def make_groups(self, dbi: DbInterface, db_id: DbId, data) -> list[DbId]:
        """Internal function called to insert groups into a given step"""
        insert_fields = dict(
            production_name=data.p_name,
            campaign_name=data.c_name,
            step_name=data.name,
            coll_source=data.coll_in,
        )
        db_id_list = []
        for group_kwargs in self._group_iterator(dbi, db_id, data, **insert_fields):
            insert_fields.update(**group_kwargs)
            dbi.insert(LevelEnum.group, db_id, self, **insert_fields)
        db_id_list += dbi.prepare(LevelEnum.group, db_id)
        return db_id_list

    def check_prerequistes(self, dbi: DbInterface, db_id: DbId) -> bool:
        """Internal function to see if the pre-requistes for a given step
        have been completed"""
        prereq_list = dbi.get_prerequisites(db_id)
        for prereq_ in prereq_list:
            status = dbi.get_status(prereq_.level(), prereq_)
            if status.value < StatusEnum.accepted.value:
                return False
        return True
