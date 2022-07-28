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
from lsst.cm.tools.core.handler import EntryHandlerBase
from lsst.cm.tools.core.utils import InputType, LevelEnum, OutputType, StatusEnum
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.handler_utils import (
    accept_children,
    accept_entry,
    check_entries,
    check_entry,
    collect_children,
    collect_entry,
    prepare_entry,
    reject_entry,
    rollback_children,
    rollback_entry,
    validate_children,
    validate_entry,
)
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow_handler import WorkflowHandler


class GroupHandler(EntryHandlerBase):

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
            coll_source=kwargs.get("coll_source"),
            input_type=InputType.tagged,
            output_type=OutputType.chained,
            status=StatusEnum.waiting,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        extra_fields = dict(
            prod_base_url=parent.prod_base_url,
        )
        coll_names = self.coll_names(insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return Group.insert_values(dbi, **insert_fields)

    def prepare(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        db_id_list = prepare_entry(dbi, self, entry)
        if not db_id_list:
            return db_id_list
        workflow_handler = self.make_workflow_handler()
        workflow_handler.insert(
            dbi,
            entry,
            workflow_idx=0,
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.s_.name,
            group_name=entry.name,
        )
        return db_id_list

    def check(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        db_id_list = check_entries(dbi, entry.w_)
        db_id_list += check_entry(dbi, entry)
        return db_id_list

    def collect(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        db_id_list = collect_children(dbi, entry.w_)
        db_id_list += collect_entry(dbi, self, entry)
        return db_id_list

    def validate(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        db_id_list = validate_children(dbi, entry.w_)
        db_id_list += validate_entry(dbi, self, entry)
        return db_id_list

    def accept(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        db_id_list = accept_children(dbi, entry.w_)
        db_id_list += accept_entry(dbi, entry)
        return db_id_list

    def reject(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        return reject_entry(dbi, entry)

    def make_workflow_handler(self) -> WorkflowHandler:
        raise NotImplementedError()

    def rollback(self, dbi: DbInterface, entry: Any, to_status: StatusEnum) -> list[DbId]:
        return rollback_entry(dbi, self, entry, to_status)

    def rollback_run(self, dbi: DbInterface, entry: Any, to_status: StatusEnum) -> list[DbId]:
        assert entry.status.value >= to_status.value
        db_id_list = rollback_children(dbi, entry.w_, to_status)
        return db_id_list
