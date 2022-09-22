import os
from typing import Any, Iterable

import yaml

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, make_bps_command, write_command_script
from lsst.cm.tools.db.job_handler import JobHandler
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.step_handler import StepHandler
from lsst.cm.tools.db.workflow import Workflow

if os.environ.get("CM_PROFILE", 0) == "1":  # pragma: no cover
    NGROUP1 = 500
    NGROUP2 = 500
    NGROUP3 = 500
else:
    NGROUP1 = 10
    NGROUP2 = 20
    NGROUP3 = 20


class ExampleJobHandler(JobHandler):
    """Example job callback handler"""

    yaml_checker_class = YamlChecker().get_checker_class_name()
    fake_rollback_class = FakeRollback().get_rollback_class_name()

    def write_job_hook(self, dbi: DbInterface, parent: Workflow, job: JobBase, **kwargs: Any) -> None:
        """Internal function to write the bps.yaml file for a given workflow"""
        if Handler.config_dir and not os.getenv("CM_CONFIGS"):
            os.environ["CM_CONFIGS"] = Handler.config_dir
        workflow_template_yaml = os.path.expandvars(self.config["bps_template_yaml"])
        butler_repo = parent.butler_repo

        outpath = job.config_url

        with open(workflow_template_yaml, "rt", encoding="utf-8") as fin:
            workflow_config = yaml.safe_load(fin)

        workflow_config["project"] = parent.p_.name
        workflow_config["campaign"] = f"{parent.p_.name}/{parent.c_.name}"

        workflow_config["pipelineYaml"] = self.config["pipeline_yaml"][parent.s_.name]
        payload = dict(
            payloadName=f"{parent.p_.name}/{parent.c_.name}",
            output=parent.coll_out,
            butlerConfig=butler_repo,
            inCollection=f"{parent.coll_in},{parent.c_.coll_ancil}",
        )
        workflow_config["payload"] = payload
        with open(outpath, "wt", encoding="utf-8") as fout:
            yaml.dump(workflow_config, fout)

        command = make_bps_command(outpath, job.json_url, job.log_url)
        write_command_script(job, command)


class ExampleStep1Handler(StepHandler):
    """Example step handler (1 of 3)"""

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(NGROUP1):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep2Handler(StepHandler):
    """Example step handler (2 of 3)"""

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(NGROUP2):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep3Handler(StepHandler):
    """Example step handler (3 of 3)"""

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(NGROUP3):
            out_dict.update(group_name=f"group{i}", data_query=f"i == {i}")
            yield out_dict
