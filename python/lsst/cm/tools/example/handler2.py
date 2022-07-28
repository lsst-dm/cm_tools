import os
from collections import OrderedDict
from typing import Any, Iterable

import yaml
from lsst.cm.tools.core.db_interface import DbInterface, JobBase, ScriptBase
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, make_bps_command, write_command_script
from lsst.cm.tools.db.campaign_handler import CampaignHandler
from lsst.cm.tools.db.group_handler import GroupHandler
from lsst.cm.tools.db.job_handler import JobHandler
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.step_handler import StepHandler
from lsst.cm.tools.db.workflow import Workflow
from lsst.cm.tools.db.workflow_handler import WorkflowHandler


class Example2Config:

    default_config = dict(
        script_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
        stamp_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
        log_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
        config_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}_bps.yaml",
        coll_in_template="prod/{fullname}_input",
        coll_out_template="prod/{fullname}_output",
        coll_validate_template="prod/{fullname}_validate",
    )


class Example2JobHandler(Example2Config, JobHandler):

    yaml_checker_class = YamlChecker().get_checker_class_name()
    fake_rollback_class = FakeRollback().get_rollback_class_name()

    def write_job_hook(self, dbi: DbInterface, parent: Workflow, job: JobBase, **kwargs: Any) -> None:
        """Internal function to write the bps.yaml file for a given workflow"""
        workflow_template_yaml = os.path.expandvars(self.config["workflow_template_yaml"])
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
            inCollection=parent.coll_in,
        )
        workflow_config["payload"] = payload
        with open(outpath, "wt", encoding="utf-8") as fout:
            yaml.dump(workflow_config, fout)

        command = make_bps_command(outpath)
        write_command_script(job, command)


class Example2WorkflowHander(Example2Config, WorkflowHandler):

    job_handler_class = Example2JobHandler().get_handler_class_name()

    def make_job_handler(self) -> JobHandler:
        return Handler.get_handler(self.job_handler_class, self.config_url)


class Example2EntryHandler(Example2Config):

    yaml_checker_class = YamlChecker().get_checker_class_name()
    fake_rollback_class = FakeRollback().get_rollback_class_name()

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


class Example2GroupHandler(Example2EntryHandler, GroupHandler):

    workflow_handler_class = Example2WorkflowHander().get_handler_class_name()

    def make_workflow_handler(self) -> WorkflowHandler:
        return Handler.get_handler(self.workflow_handler_class, self.config_url)


class Example2Step1Handler(Example2EntryHandler, StepHandler):

    group_handler_class = Example2GroupHandler().get_handler_class_name()

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(10):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class Example2Step2Handler(Example2EntryHandler, StepHandler):

    group_handler_class = Example2GroupHandler().get_handler_class_name()

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(20):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class Example2Step3Handler(Example2EntryHandler, StepHandler):

    group_handler_class = Example2GroupHandler().get_handler_class_name()

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(20):
            out_dict.update(group_name=f"group{i}", data_query=f"i == {i}")
            yield out_dict


class Example2Handler(Example2EntryHandler, CampaignHandler):

    step_dict = OrderedDict(
        [
            ("step1", Example2Step1Handler),
            ("step2", Example2Step2Handler),
            ("step3", Example2Step3Handler),
        ]
    )
