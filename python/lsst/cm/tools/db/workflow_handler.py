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
from lsst.cm.tools.core.handler import WorkflowHandlerBase
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, write_status_to_yaml
from lsst.cm.tools.core.utils import ScriptMethod, StatusEnum
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.workflow import Workflow


class WorkflowHandler(WorkflowHandlerBase):
    """Campaign level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement the `write_workflow_hook` function to write the
    configuration and shell scripts to run the workflow
    2. define the `Checker` and `Rollback` classes
    """

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

    checker_class_name = YamlChecker().get_checker_class_name()
    rollback_class_name = FakeRollback().get_rollback_class_name()

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
            checker=kwargs.get("checker", self.checker_class_name),
            rollback=kwargs.get("rollback", self.rollback_class_name),
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        script_data = self.resolve_templated_strings(
            self.run_script_url_template_names,
            prod_base_url=parent.prod_base_url,
            fullname=parent.fullname,
            idx=workflow_idx,
            name="run",
        )
        insert_fields.update(**script_data)
        workflow = Workflow.insert_values(dbi, **insert_fields)
        self.write_workflow_hook(dbi, parent, workflow, **insert_fields)

    def launch(self, dbi: DbInterface, workflow: Workflow) -> None:
        submit_command = f"{workflow.script_url} {workflow.config_url}"
        # workflow_start = datetime.now()
        print(f"Submitting workflow {str(workflow.db_id)} with {submit_command}")
        update_fields = dict(status=StatusEnum.running)
        Workflow.update_values(dbi, workflow.id, **update_fields)

    def fake_run_hook(
        self, dbi: DbInterface, workflow: Workflow, status: StatusEnum = StatusEnum.completed
    ) -> None:
        write_status_to_yaml(workflow.log_url, status)
