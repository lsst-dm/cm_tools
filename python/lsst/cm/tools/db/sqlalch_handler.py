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
