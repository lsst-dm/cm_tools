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
from typing import Any, Iterable

from lsst.cm.tools.core.db_interface import DbInterface, JobBase, ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.entry_handler import EntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.handler_utils import prepare_entry
from lsst.cm.tools.db.job_handler import JobHandler
from lsst.cm.tools.db.workflow import Workflow


class WorkflowHandler(EntryHandler):
    """Campaign level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement the `write_job_hook` function to write the
    configuration and shell scripts to run the workflow
    """

    config_block = "workflow"

    fullname_template = os.path.join(
        "{production_name}",
        "{campaign_name}",
        "{step_name}",
        "{group_name}",
        "w{workflow_idx:02}",
    )

    level = LevelEnum.workflow

    def insert(self, dbi: DbInterface, parent: Group, **kwargs: Any) -> Workflow:
        workflow_idx = len(parent.w_)
        insert_fields = dict(
            name="%02i" % workflow_idx,
            fullname=self.get_fullname(workflow_idx=workflow_idx, **kwargs),
            p_id=parent.p_.id,
            c_id=parent.c_.id,
            s_id=parent.s_.id,
            g_id=parent.id,
            idx=workflow_idx,
            coll_in=parent.coll_in,
            data_query=kwargs.get("data_query"),
            status=StatusEnum.ready,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        workflow = Workflow.insert_values(dbi, **insert_fields)
        return workflow

    def prepare(self, dbi: DbInterface, entry: Group) -> list[DbId]:
        db_id_list = prepare_entry(dbi, self, entry)
        if not db_id_list:
            return db_id_list
        job_handler = self.make_job_handler()
        job_handler.insert(
            dbi,
            entry,
            name="run",
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.s_.name,
            group_name=entry.name,
        )
        return db_id_list

    def make_job_handler(self) -> JobHandler:
        raise NotImplementedError()

    def prepare_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        assert dbi
        assert entry
        return []

    def collect_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        assert dbi
        assert entry
        return []

    def validate_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        assert dbi
        assert entry
        return []

    def accept_hook(self, dbi: DbInterface, itr: Iterable, entry: Any) -> None:
        pass

    def reject_hook(self, dbi: DbInterface, entry: Any) -> None:
        pass

    def run_hook(self, dbi: DbInterface, entry: Any) -> list[JobBase]:
        current_status = entry.status
        db_id_list: list[DbId] = []
        if current_status != StatusEnum.prepared:
            return db_id_list
        for job in entry.jobs_:
            job.update_values(dbi, job.id, status=StatusEnum.ready)
            db_id_list.append(job.w_.db_id)
        return db_id_list
