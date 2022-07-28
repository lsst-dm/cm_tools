from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.handler import JobHandlerBase
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, write_status_to_yaml
from lsst.cm.tools.core.utils import ScriptMethod, StatusEnum
from lsst.cm.tools.db.job import Job


class JobHandler(JobHandlerBase):
    """Job callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement `write_job_hook` to write the script to run
    2. implement `launch_hook` to launch the job
    """

    default_config = dict(
        script_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
        stamp_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
        log_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
        config_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}_bps.yaml",
    )

    job_url_template_names = dict(
        script_url="script_url_template",
        log_url="log_url_template",
        stamp_url="stamp_url_template",
        config_url="config_url_template",
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
            c_id=parent.db_id.c_id,
            s_id=parent.db_id.s_id,
            g_id=parent.db_id.g_id,
            w_id=parent.db_id.w_id,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
            checker=self.checker_class_name,
            rollback=self.rollback_class_name,
            status=StatusEnum.waiting,
            script_method=self.script_method,
            level=parent.level,
        )
        script_data = self.resolve_templated_strings(
            self.job_url_template_names,
            prod_base_url=parent.prod_base_url,
            fullname=parent.fullname,
            idx=idx,
            name=name,
        )
        insert_fields.update(**script_data)
        script = Job.insert_values(dbi, **insert_fields)
        self.write_job_hook(dbi, parent, script, **kwcopy)
        return script

    def fake_run_hook(
        self, dbi: DbInterface, job: JobBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        write_status_to_yaml(job.stamp_url, status)

    def launch(self, dbi: DbInterface, job: JobBase) -> StatusEnum:
        Job.update_values(dbi, job.id, status=StatusEnum.running)
