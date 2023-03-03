import os
from typing import Any

import yaml

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.handler import JobHandlerBase
from lsst.cm.tools.core.panda_utils import PandaChecker
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, make_bps_command, write_command_script
from lsst.cm.tools.core.slurm_utils import submit_job
from lsst.cm.tools.core.utils import ScriptMethod, StatusEnum
from lsst.cm.tools.db.job import Job
from lsst.cm.tools.db.workflow import Workflow


class JobHandler(JobHandlerBase):
    """Job callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement `write_job_hook` to write the script to run
    """

    default_config = dict(
        templates=dict(
            script_url="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
            stamp_url="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
            log_url="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
            config_url="{prod_base_url}/{fullname}/{name}_{idx:03}_bps.yaml",
            json_url="{prod_base_url}/{fullname}/{name}_{idx:03}.json",
            coll_out="{root_coll}/{fullname}_{idx:03}",
        )
    )

    checker_class_dict = {
        ScriptMethod.fake_run: None,
        ScriptMethod.no_run: None,
        ScriptMethod.no_script: None,
        ScriptMethod.bash: YamlChecker,
        ScriptMethod.slurm: PandaChecker,
    }

    rollback_class_name = FakeRollback().get_rollback_class_name()

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> JobBase:
        kwcopy = kwargs.copy()
        name = kwcopy.pop("name")
        prev_jobs = [job for job in parent.jobs_ if job.name == name]
        idx = len(prev_jobs)
        checker_class = self.checker_class_dict[self.script_method]
        if checker_class is None:  # pragma: no cover
            checker_class_name = None
        else:
            checker_class_name = checker_class().get_checker_class_name()
        insert_fields = dict(
            name=name,
            idx=idx,
            p_id=parent.db_id.p_id,
            c_id=parent.db_id.c_id,
            s_id=parent.db_id.s_id,
            g_id=parent.db_id.g_id,
            w_id=parent.db_id.w_id,
            frag_id=self._fragment_id,
            checker=checker_class_name,
            rollback=self.rollback_class_name,
            coll_out=f"{parent.c_.root_coll}/{parent.fullname}_{idx:03}",
            status=StatusEnum.ready,
            bps_yaml_template=self.get_config_var("bps_yaml_template", parent.bps_yaml_template, **kwcopy),
            bps_script_template=self.get_config_var(
                "bps_script_template", parent.bps_script_template, **kwcopy
            ),
            pipeline_yaml=self.get_config_var("pipeline_yaml", parent.pipeline_yaml, **kwcopy),
            lsst_version=self.get_config_var("lsst_version", parent.lsst_version, **kwcopy),
            lsst_custom_setup=self.get_config_var("lsst_custom_setup", parent.lsst_custom_setup, **kwcopy),
            script_method=self.script_method,
            level=parent.level,
        )
        script_data = self.resolve_templated_strings(
            prod_base_url=parent.prod_base_url,
            root_coll=parent.c_.root_coll,
            fullname=parent.fullname,
            idx=idx,
            name=name,
        )
        if self.script_method == ScriptMethod.slurm:  # pragma: no cover
            script_data.pop("stamp_url")
        insert_fields.update(**script_data)
        new_job = Job.insert_values(dbi, **insert_fields)
        dbi.connection().commit()
        return new_job

    def write_job_hook(self, dbi: DbInterface, parent: Workflow, job: JobBase, **kwargs: Any) -> None:
        """Internal function to write the bps.yaml file for a given workflow"""
        workflow_template_yaml = os.path.expandvars(job.bps_yaml_template)
        butler_repo = parent.butler_repo

        outpath = job.config_url

        with open(workflow_template_yaml, "rt", encoding="utf-8") as fin:
            workflow_config = yaml.safe_load(fin)

        workflow_config["project"] = parent.p_.name
        workflow_config["campaign"] = f"{parent.p_.name}/{parent.c_.name}"
        workflow_config["LSST_VERSION"] = job.lsst_version
        workflow_config["lsst_custom_setup"] = job.lsst_custom_setup
        workflow_config["pipelineYaml"] = job.pipeline_yaml
        payload = dict(
            payloadName=f"{parent.p_.name}/{parent.c_.name}",
            outputRun=job.coll_out,
            butlerConfig=butler_repo,
            inCollection=f"{parent.coll_in},{parent.c_.coll_in},{parent.c_.coll_ancil}",
        )
        if parent.data_query:
            payload["dataQuery"] = parent.data_query

        workflow_config["payload"] = payload

        if parent.get_handler().config.get("rescue", False):
            workflow_config["extraQgraphOptions"] = f"--clobber-outputs --skip-existing-in {parent.coll_out}"

        with open(outpath, "wt", encoding="utf-8") as fout:
            yaml.dump(workflow_config, fout)

        try:
            bps_script_template = os.path.expandvars(job.bps_script_template)
            with open(bps_script_template, "r") as fin:
                prepend = fin.read().replace("{lsst_version}", parent.c_.lsst_version)
        except KeyError:
            prepend = ""

        command = make_bps_command(outpath, job.json_url, job.log_url)
        write_command_script(job, command, prepend=prepend)

    def fake_run_hook(
        self, dbi: DbInterface, job: JobBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        job.update_values(dbi, job.id, status=status)

    def launch(self, dbi: DbInterface, job: JobBase) -> StatusEnum:
        parent = job.w_
        if job.script_method == ScriptMethod.fake_run:  # pragma: no cover
            status = StatusEnum.running
        elif job.script_method == ScriptMethod.no_run:  # pragma: no cover
            status = StatusEnum.ready
        elif job.script_method == ScriptMethod.no_script:  # pragma: no cover
            status = StatusEnum.running
        elif job.script_method == ScriptMethod.bash:
            os.system(f"source {job.script_url}")
            status = StatusEnum.running
        elif job.script_method == ScriptMethod.slurm:  # pragma: no coveres
            job_id = submit_job(job.script_url, job.log_url)
            Job.update_values(dbi, job.id, stamp_url=job_id)
            status = StatusEnum.running
        parent = job.w_
        Job.update_values(dbi, job.id, status=status)
        Workflow.update_values(dbi, parent.id, status=StatusEnum.running)
        return status
