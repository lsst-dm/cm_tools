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

from __future__ import annotations

import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import EntryHandlerBase
from lsst.cm.tools.core.script_utils import write_status_to_yaml
from lsst.cm.tools.core.utils import ScriptMethod, StatusEnum
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.handler_utils import (
    accept_entry,
    check_entry,
    collect_entry,
    reject_entry,
    rollback_entry,
    validate_entry,
)
from lsst.cm.tools.db.workflow import Workflow


class WorkflowHandler(EntryHandlerBase):

    run_script_url_template_names = dict(
        script_url="script_url_template",
        log_url="log_url_template",
        config_url="config_url_template",
    )

    fullname_template = os.path.join(
        "{production_name}",
        "{campaign_name}",
        "{step_name}",
        "{group_name}_w{workflow_idx}",
    )

    def insert(self, dbi: DbInterface, parent: Group, **kwargs: Any) -> Workflow:
        workflow_idx = self.get_kwarg_value("workflow_idx", **kwargs)
        insert_fields = dict(
            name="%02i" % workflow_idx,
            fullname=self.get_fullname(**kwargs),
            p_id=parent.p_.id,
            c_id=parent.c_.id,
            s_id=parent.s_.id,
            g_id=parent.id,
            idx=workflow_idx,
            data_query=kwargs.get("data_query"),
            coll_in=parent.coll_in,
            coll_out=parent.coll_out,
            status=StatusEnum.ready,
            script_method=ScriptMethod.bash_stamp,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        workflow = Workflow.insert_values(dbi, **insert_fields)
        return workflow

    def launch(self, dbi: DbInterface, workflow: Workflow) -> None:
        submit_command = f"{workflow.script_url} {workflow.config_url}"
        # workflow_start = datetime.now()
        print(f"Submitting workflow {str(workflow.db_id)} with {submit_command}")
        update_fields = dict(status=StatusEnum.running)
        Workflow.update_values(dbi, workflow.id, **update_fields)

    def fake_run_hook(self, dbi: DbInterface, entry: Any, status: StatusEnum = StatusEnum.completed) -> None:
        write_status_to_yaml(entry.log_url, status)

    def check(self, dbi: DbInterface, entry: Workflow) -> list[DbId]:
        db_id_list = check_entry(dbi, entry)
        return db_id_list

    def collect(self, dbi: DbInterface, entry: Workflow) -> list[DbId]:
        db_id_list = collect_entry(dbi, self, entry)
        return db_id_list

    def validate(self, dbi: DbInterface, entry: Workflow) -> list[DbId]:
        db_id_list = validate_entry(dbi, self, entry)
        return db_id_list

    def accept(self, dbi: DbInterface, entry: Workflow) -> list[DbId]:
        db_id_list = accept_entry(dbi, entry)
        return db_id_list

    def reject(self, dbi: DbInterface, entry: Workflow) -> list[DbId]:
        return reject_entry(dbi, entry)

    def make_workflow_handler(self) -> WorkflowHandler:
        raise NotImplementedError()

    def rollback(self, dbi: DbInterface, entry: Any, to_status: StatusEnum) -> list[DbId]:
        return rollback_entry(dbi, self, entry, to_status)

    def rollback_run(self, dbi: DbInterface, entry: Any, to_status: StatusEnum) -> list[DbId]:
        assert entry.status.value >= to_status.value
        db_id_list = rollback_entry(dbi, entry.w_, to_status)
        return db_id_list
