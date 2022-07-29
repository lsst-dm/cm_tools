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

import os
from typing import Any, Iterable, Optional

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.entry_handler import EntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.handler_utils import prepare_entry
from lsst.cm.tools.db.step import Step


class StepHandler(EntryHandler):
    """Campaign level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement the `xxx_hook` functions.
    2. define the Group callback hander with `group_handler_class`
    3. provide the parameters for the Group callback handler with the
    `group_iterator` function.
    """

    config_block = "step"

    fullname_template = os.path.join("{production_name}", "{campaign_name}", "{step_name}")

    group_handler_class: Optional[str]

    level = LevelEnum.step

    def insert(self, dbi: DbInterface, parent: Campaign, **kwargs: Any) -> Step:
        insert_fields = dict(
            name=self.get_kwarg_value("step_name", **kwargs),
            fullname=self.get_fullname(**kwargs),
            p_id=parent.p_.id,
            c_id=parent.id,
            data_query=kwargs.get("data_query"),
            coll_in=parent.coll_in,
            coll_source=parent.coll_in,
            status=StatusEnum.waiting,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        extra_fields = dict(
            prod_base_url=parent.prod_base_url,
            root_coll=parent.root_coll,
        )
        coll_names = self.coll_names(insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return Step.insert_values(dbi, **insert_fields)

    def prepare(self, dbi: DbInterface, entry: Step) -> list[DbId]:
        db_id_list = prepare_entry(dbi, self, entry)
        if not db_id_list:
            return db_id_list
        self.make_groups(dbi, entry)
        db_id_list.append(entry.db_id)
        for group_ in entry.g_:
            status = group_.status
            if status == StatusEnum.waiting:
                if not group_.check_prerequistes(dbi):  # pragma: no cover
                    raise RuntimeError("We are not expecting groups to have prerequistes")
            group_handler = group_.get_handler()
            db_id_list += group_handler.prepare(dbi, group_)
        return db_id_list

    def make_groups(self, dbi: DbInterface, entry: Step) -> dict[str, Group]:
        out_dict = {}
        insert_fields = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
            coll_source=entry.coll_in,
        )
        group_handler = Handler.get_handler(self.group_handler_class, entry.config_yaml)
        for group_kwargs in self.group_iterator(dbi, entry, **insert_fields):
            insert_fields.update(**group_kwargs)
            out_dict[group_kwargs["group_name"]] = group_handler.insert(dbi, entry, **insert_fields)
        return out_dict

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        raise NotImplementedError()
