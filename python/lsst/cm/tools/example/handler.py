import os
from collections import OrderedDict
from typing import Any, Iterable

from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase, WorkflowBase
from lsst.cm.tools.core.handler import EntryHandlerBase, Handler
from lsst.cm.tools.core.script_utils import (
    YamlChecker,
    add_command_script,
    make_bps_command,
    make_butler_associate_command,
    make_butler_chain_command,
    write_command_script,
)
from lsst.cm.tools.core.utils import ScriptMethod, ScriptType
from lsst.cm.tools.db.campaign_handler import CampaignHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.group_handler import GroupHandler
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.step_handler import StepHandler
from lsst.cm.tools.db.workflow_handler import WorkflowHandler


def prepare_script(dbi: DbInterface, handler: EntryHandlerBase, entry: Any) -> list[ScriptBase]:
    idx = 0
    script_name = "prepare"
    script_data = handler.resolve_templated_strings(
        handler.prepare_script_url_template_names,
        prod_base_url=entry.prod_base_url,
        fullname=entry.fullname,
        idx=idx,
        name=script_name,
    )
    script_data.update(
        p_id=entry.db_id.p_id,
        c_id=entry.db_id.c_id,
        s_id=entry.db_id.s_id,
        g_id=entry.db_id.g_id,
        idx=idx,
        name=script_name,
        level=entry.level,
        coll_out=entry.coll_in,
        script_type=ScriptType.prepare,
        script_method=ScriptMethod.bash_stamp,
    )
    command = make_butler_associate_command(entry.butler_repo, entry)
    script = add_command_script(
        dbi,
        command,
        script_data,
        checker=handler.yaml_checker_class,
        fake_stamp=True,
    )
    return [script]


def collect_script(
    dbi: DbInterface, handler: EntryHandlerBase, itr: Iterable, entry: Any
) -> list[ScriptBase]:
    idx = 0
    script_name = "collect"
    script_data = handler.resolve_templated_strings(
        handler.collect_script_url_template_names,
        prod_base_url=entry.prod_base_url,
        fullname=entry.fullname,
        idx=idx,
        name=script_name,
    )
    script_data.update(
        p_id=entry.db_id.p_id,
        c_id=entry.db_id.c_id,
        s_id=entry.db_id.s_id,
        g_id=entry.db_id.g_id,
        idx=0,
        name=script_name,
        level=entry.level,
        coll_out=entry.coll_out,
        script_type=ScriptType.collect,
        script_method=ScriptMethod.bash_stamp,
    )
    command = make_butler_chain_command(entry.butler_repo, entry, itr)
    script = add_command_script(
        dbi,
        command,
        script_data,
        checker=handler.yaml_checker_class,
        fake_stamp=True,
    )
    return [script]


class ExampleConfig:

    default_config = dict(
        script_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
        stamp_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
        log_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
        config_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}_bps.yaml",
        coll_in_template="prod/{fullname}_input",
        coll_out_template="prod/{fullname}_output",
    )

    prepare_script_url_template_names = dict(
        script_url="script_url_template",
        log_url="log_url_template",
    )

    collect_script_url_template_names = dict(
        script_url="script_url_template",
        log_url="log_url_template",
    )

    run_script_url_template_names = dict(
        script_url="script_url_template",
        log_url="log_url_template",
        config_url="config_url_template",
    )


class ExampleWorkflowHander(ExampleConfig, WorkflowHandler):

    yaml_checker_class = YamlChecker().get_checker_class_name()

    def workflow_script_hook(self, dbi: DbInterface, parent: Group, **kwargs: Any) -> WorkflowBase:
        """Internal function to write the bps.yaml file for a given workflow"""
        workflow_template_yaml = os.path.expandvars(self.config["workflow_template_yaml"])
        butler_repo = parent.butler_repo

        workflow = self.insert(
            dbi,
            parent,
            checker=self.yaml_checker_class,
            **kwargs,
        )

        outpath = workflow.config_url
        import yaml

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
        write_command_script(workflow, command, fake_stamp=True)
        return workflow


class ExampleEntryHandler(ExampleConfig):

    yaml_checker_class = YamlChecker().get_checker_class_name()

    def prepare_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        return prepare_script(dbi, self, entry)

    def collect_script_hook(self, dbi: DbInterface, itr: Iterable, entry: Any) -> list[ScriptBase]:
        return collect_script(dbi, self, itr, entry)

    def accept_hook(self, dbi: DbInterface, itr: Iterable, entry: Any) -> None:
        return

    def reject_hook(self, dbi: DbInterface, entry: Any) -> None:
        return


class ExampleGroupHandler(ExampleEntryHandler, GroupHandler):

    workflow_handler_class = ExampleWorkflowHander().get_handler_class_name()

    def make_workflow_handler(self) -> WorkflowHandler:
        return Handler.get_handler(self.workflow_handler_class, self.config_url)


class ExampleStep1Handler(ExampleEntryHandler, StepHandler):

    group_handler_class = ExampleGroupHandler().get_handler_class_name()

    def _group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
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

    def _group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(10):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep3Handler(ExampleEntryHandler, StepHandler):

    group_handler_class = ExampleGroupHandler().get_handler_class_name()

    def _group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(10):
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
