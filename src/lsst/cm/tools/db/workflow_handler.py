from __future__ import annotations

import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.entry_handler import GenericEntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.handler_utils import rollback_jobs
from lsst.cm.tools.db.workflow import Workflow


class WorkflowHandler(GenericEntryHandler):
    """Campaign level callback handler

    Provides interface functions.
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
        kwcopy = kwargs.copy()
        workflow_idx = len(parent.w_)
        kwcopy["workflow_idx"] = workflow_idx
        insert_fields = dict(
            name=f"{workflow_idx:02}",
            fullname=self.get_fullname(**kwcopy),
            p_id=parent.p_.id,
            c_id=parent.c_.id,
            s_id=parent.s_.id,
            g_id=parent.id,
            idx=workflow_idx,
            config_id=parent.config_id,
            frag_id=self._fragment_id,
            coll_in=parent.coll_in,
            coll_out=parent.coll_out,
            data_query=kwcopy.get("data_query"),
            status=StatusEnum.waiting,
        )
        workflow = Workflow.insert_values(dbi, **insert_fields)
        return workflow

    def requeue_job(self, dbi: DbInterface, entry: Any, job_name="job") -> None:
        job_handler = entry.get_sub_handler(job_name)
        job_handler.insert(
            dbi,
            entry,
            name=job_name,
        )

    def _make_jobs(self, dbi: DbInterface, entry: Any) -> None:
        job_handler = entry.get_sub_handler(self.config.get("job_config", "job"))
        job_handler.insert(
            dbi,
            entry,
            name="job",
        )
        return StatusEnum.ready

    def make_children(self, dbi: DbInterface, entry: Workflow) -> StatusEnum:
        return StatusEnum.populating

    def supersede_hook(self, dbi: DbInterface, entry: Any) -> None:
        rollback_jobs(dbi, entry)
