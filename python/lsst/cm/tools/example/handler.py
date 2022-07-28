import os
from collections import OrderedDict
from typing import Any, Iterable

import yaml
from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase, WorkflowBase
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, make_bps_command, write_command_script
from lsst.cm.tools.core.utils import StatusEnum
from lsst.cm.tools.db.campaign_handler import CampaignHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.group_handler import GroupHandler
from lsst.cm.tools.db.script_handler import CollectScriptHandler, PrepareScriptHandler, ValidateScriptHandler
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.step_handler import StepHandler
from lsst.cm.tools.db.workflow_handler import WorkflowHandler


class ExampleConfig:

    default_config = dict(
        script_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
        stamp_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
        log_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
        config_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}_bps.yaml",
        coll_in_template="prod/{fullname}_input",
        coll_out_template="prod/{fullname}_output",
        coll_validate_template="prod/{fullname}_validate",
    )


class ExampleWorkflowHander(ExampleConfig, WorkflowHandler):

    yaml_checker_class = YamlChecker().get_checker_class_name()
    fake_rollback_class = FakeRollback().get_rollback_class_name()

    def write_workflow_hook(
        self, dbi: DbInterface, parent: Group, workflow: WorkflowBase, **kwargs: Any
    ) -> None:
        """Internal function to write the bps.yaml file for a given workflow"""
        workflow_template_yaml = os.path.expandvars(self.config["workflow_template_yaml"])
        butler_repo = parent.butler_repo

        outpath = workflow.config_url

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
        write_command_script(workflow, command)


class ExampleEntryHandler(ExampleConfig):

    yaml_checker_class = YamlChecker().get_checker_class_name()
    fake_rollback_class = FakeRollback().get_rollback_class_name()

    prepare_handler_class = PrepareScriptHandler().get_handler_class_name()
    collect_handler_class = CollectScriptHandler().get_handler_class_name()
    validate_handler_class = ValidateScriptHandler().get_handler_class_name()

    def prepare_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        handler = Handler.get_handler(self.prepare_handler_class, entry.config_yaml)
        script = handler.insert(
            dbi,
            entry,
            name="prepare",
            idx=0,
            prepend=f"# Written by {handler.get_handler_class_name()}",
            append="# Have a good day",
            fake_run=StatusEnum.completed,
        )
        return [script]

    def collect_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        handler = Handler.get_handler(self.collect_handler_class, entry.config_yaml)
        script = handler.insert(
            dbi,
            entry,
            name="collect",
            idx=0,
            prepend=f"# Written by {handler.get_handler_class_name()}",
            append="# Have a good day",
            fake_run=StatusEnum.completed,
        )
        return [script]

    def validate_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        handler = Handler.get_handler(self.validate_handler_class, entry.config_yaml)
        script = handler.insert(
            dbi,
            entry,
            name="validate",
            idx=0,
            prepend=f"# Written by {handler.get_handler_class_name()}",
            append="# Have a good day",
            fake_run=StatusEnum.completed,
        )
        return [script]

    def accept_hook(self, dbi: DbInterface, itr: Iterable, entry: Any) -> None:
        pass

    def reject_hook(self, dbi: DbInterface, entry: Any) -> None:
        pass


class ExampleGroupHandler(ExampleEntryHandler, GroupHandler):

    workflow_handler_class = ExampleWorkflowHander().get_handler_class_name()

    def make_workflow_handler(self) -> WorkflowHandler:
        return Handler.get_handler(self.workflow_handler_class, self.config_url)


class ExampleStep1Handler(ExampleEntryHandler, StepHandler):

    group_handler_class = ExampleGroupHandler().get_handler_class_name()

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(10):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep2Handler(ExampleEntryHandler, StepHandler):

    group_handler_class = ExampleGroupHandler().get_handler_class_name()

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(20):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep3Handler(ExampleEntryHandler, StepHandler):

    group_handler_class = ExampleGroupHandler().get_handler_class_name()

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(20):
            out_dict.update(group_name=f"group{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleHandler(ExampleEntryHandler, CampaignHandler):

    step_dict = OrderedDict(
        [
            ("step1", ExampleStep1Handler),
            ("step2", ExampleStep2Handler),
            ("step3", ExampleStep3Handler),
        ]
    )
