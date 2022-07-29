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
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.entry_handler import EntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.handler_utils import prepare_entry
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow_handler import WorkflowHandler


class GroupHandler(EntryHandler):
    """Group level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement the `xxx_hook` functions.
    2. define the Workflow callback hander with `make_workflow_handler`
    """

    config_block = "group"

    fullname_template = os.path.join(
        "{production_name}",
        "{campaign_name}",
        "{step_name}",
        "{group_name}",
    )

    level = LevelEnum.group

    def insert(self, dbi: DbInterface, parent: Step, **kwargs: Any) -> Group:
        insert_fields = dict(
            name=self.get_kwarg_value("group_name", **kwargs),
            fullname=self.get_fullname(**kwargs),
            p_id=parent.p_.id,
            c_id=parent.c_.id,
            s_id=parent.id,
            data_query=kwargs.get("data_query"),
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
        return Group.insert_values(dbi, **insert_fields)

    def prepare(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        db_id_list = prepare_entry(dbi, self, entry)
        if not db_id_list:
            return db_id_list
        workflow_handler = self.make_workflow_handler()
        workflow = workflow_handler.insert(
            dbi,
            entry,
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.s_.name,
            group_name=entry.name,
        )
        db_id_list.append(workflow.db_id)
        return db_id_list

    def make_workflow_handler(self) -> WorkflowHandler:
        """Return a WorkflowHandler to manage the
        Workflows associated with the Groups managed by this
        handler
        """
        raise NotImplementedError()
