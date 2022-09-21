import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.handler import JobHandlerBase
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, write_status_to_yaml
from lsst.cm.tools.core.slurm_utils import submit_job
from lsst.cm.tools.core.utils import ScriptMethod, StatusEnum
from lsst.cm.tools.db.job import Job
from lsst.cm.tools.db.workflow import Workflow


class JobHandler(JobHandlerBase):
    """Job callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement `write_job_hook` to write the script to run
    2. implement `launch_hook` to launch the job
    """

    default_config = dict(
        templates=dict(
            script_url="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
            stamp_url="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
            log_url="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
            config_url="{prod_base_url}/{fullname}/{name}_{idx:03}_bps.yaml",
        )
    )

    script_method = ScriptMethod.bash
    checker_class_name = YamlChecker().get_checker_class_name()
    rollback_class_name = FakeRollback().get_rollback_class_name()

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> JobBase:
        kwcopy = kwargs.copy()
        name = kwcopy.pop("name")
        prev_jobs = [job for job in parent.jobs_ if job.name == name]
        idx = len(prev_jobs)
        insert_fields = dict(
            name=name,
            idx=idx,
            p_id=parent.db_id.p_id,
            c_id=parent.db_id.c_id,
            s_id=parent.db_id.s_id,
            g_id=parent.db_id.g_id,
            w_id=parent.db_id.w_id,
            frag_id=self._fragment_id,
            checker=self.checker_class_name,
            rollback=self.rollback_class_name,
            coll_out=parent.coll_out,
            status=StatusEnum.ready,
            script_method=self.script_method,
            level=parent.level,
        )
        script_data = self.resolve_templated_strings(
            prod_base_url=parent.prod_base_url,
            fullname=parent.fullname,
            idx=idx,
            name=name,
        )
        insert_fields.update(**script_data)
        new_job = Job.insert_values(dbi, **insert_fields)
        dbi.connection().commit()
        return new_job

    def fake_run_hook(
        self, dbi: DbInterface, job: JobBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        write_status_to_yaml(job.stamp_url, status)

    def launch(self, dbi: DbInterface, job: JobBase) -> StatusEnum:
        parent = job.w_
        if job.script_method == ScriptMethod.no_run:  # pragma: no cover
            status = StatusEnum.ready
        elif job.script_method == ScriptMethod.no_script:  # pragma: no cover
            status = StatusEnum.running
        elif job.script_method == ScriptMethod.bash:
            os.system(f"source {job.script_url}")
        elif job.script_method == ScriptMethod.slurm:
            job_id = submit_job(job.script_url, job.log_url)
            Job.update_values(dbi, job.id, stamp_url=job_id)
        parent = job.w_
        Job.update_values(dbi, job.id, status=status)
        Workflow.update_values(dbi, parent.id, status=StatusEnum.running)
        return status
