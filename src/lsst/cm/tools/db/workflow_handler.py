from __future__ import annotations

import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.entry_handler import EntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.handler_utils import rollback_jobs
from lsst.cm.tools.db.job_handler import JobHandler
from lsst.cm.tools.db.workflow import Workflow


class WorkflowHandler(EntryHandler):
    """Campaign level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement the `make_job_handler` function to make
    the handler that writes the configuration and shell
    scripts to run the workflow
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
            name=f"{workflow_idx:02}",
            fullname=self.get_fullname(workflow_idx=workflow_idx, **kwargs),
            p_id=parent.p_.id,
            c_id=parent.c_.id,
            s_id=parent.s_.id,
            g_id=parent.id,
            idx=workflow_idx,
            coll_in=parent.coll_in,
            coll_out=parent.coll_out,
            data_query=kwargs.get("data_query"),
            status=StatusEnum.waiting,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        workflow = Workflow.insert_values(dbi, **insert_fields)
        return workflow

    def _make_jobs(self, dbi: DbInterface, entry: Any) -> None:
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
        return StatusEnum.ready

    def make_children(self, dbi: DbInterface, entry: Workflow) -> StatusEnum:
        return StatusEnum.populating

    def make_job_handler(self) -> JobHandler:
        """Return a JobHandler to manage the
        Jobs associated with the Workflows managed by this
        handler
        """
        raise NotImplementedError()

    def supersede_hook(self, dbi: DbInterface, entry: Any) -> None:
        rollback_jobs(dbi, entry)
