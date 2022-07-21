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

import os
from collections import OrderedDict
from typing import Iterable, Optional

from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase
from lsst.cm.tools.core.grouper import Grouper
from lsst.cm.tools.core.script_utils import (
    YamlChecker,
    make_butler_associate_command,
    make_butler_chain_command,
    write_status_to_yaml,
)
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.sqlalch_handler import SQLAlchemyHandler


class ExampleStep1Grouper(Grouper):
    def _do_call(self):
        out_dict = dict(
            production_name=self.config["production_name"],
            campaign_name=self.config["campaign_name"],
            step_name=self.config["step_name"],
        )

        for i in range(10):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep2Grouper(Grouper):
    def _do_call(self):
        out_dict = dict(
            production_name=self.config["production_name"],
            campaign_name=self.config["campaign_name"],
            step_name=self.config["step_name"],
        )

        for i in range(20):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep3Grouper(Grouper):
    def _do_call(self):
        out_dict = dict(
            production_name=self.config["production_name"],
            campaign_name=self.config["campaign_name"],
            step_name=self.config["step_name"],
        )

        for i in range(20):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleHandler(SQLAlchemyHandler):

    default_config = SQLAlchemyHandler.default_config.copy()

    default_config.update(
        prepare_script_url_template="{prod_base_url}/{fullname}/prepare.sh",
        prepare_log_url_template="{prod_base_url}/{fullname}/prepare.log",
        collect_script_url_template="{prod_base_url}/{fullname}/collect.sh",
        collect_log_url_template="{prod_base_url}/{fullname}/collect.log",
        run_script_url_template="bps",
        run_log_url_template="{prod_base_url}/{fullname}/run.log",
        run_config_url_template="{prod_base_url}/{fullname}/bps.yaml",
        coll_in_template="{prod_base_url}/{fullname}_input",
        coll_out_template="{prod_base_url}/{fullname}_output",
    )

    step_dict = OrderedDict(
        [("step1", ExampleStep1Grouper), ("step2", ExampleStep2Grouper), ("step3", ExampleStep3Grouper)]
    )

    coll_template_names = dict(
        coll_in="coll_in_template",
        coll_out="coll_out_template",
    )

    prepare_script_url_template_names = dict(
        script_url="prepare_script_url_template",
        log_url="prepare_log_url_template",
    )

    collect_script_url_template_names = dict(
        script_url="collect_script_url_template",
        log_url="collect_log_url_template",
    )

    run_script_url_template_names = dict(
        script_url="run_script_url_template",
        log_url="run_log_url_template",
        config_url="run_config_url_template",
    )

    yaml_checker_class = YamlChecker().get_checker_class_name()

    def coll_name_hook(self, level: LevelEnum, insert_fields: dict, **kwargs) -> dict[str, str]:
        return self.resolve_templated_strings(self.coll_template_names, insert_fields, **kwargs)

    def prepare_script_hook(
        self, level: LevelEnum, dbi: DbInterface, data
    ) -> Optional[ScriptBase]:
        assert level.value >= LevelEnum.campaign.value
        if level == LevelEnum.workflow:
            return None
        butler_repo = dbi.get_repo(data.db_id)
        script_data = self.resolve_templated_strings(
            self.prepare_script_url_template_names,
            {},
            prod_base_url=dbi.get_prod_base(data.db_id),
            fullname=data.fullname,
        )
        script = dbi.add_script(checker=self.yaml_checker_class, **script_data)
        with open(script.script_url, "wt", encoding="utf-8") as fout:
            fout.write(make_butler_associate_command(butler_repo, data))
            fout.write("\n")
        write_status_to_yaml(script.log_url, StatusEnum.completed)
        return script

    def workflow_script_hook(self, dbi: DbInterface, data, **kwargs) -> Optional[ScriptBase]:
        """Internal function to write the bps.yaml file for a given workflow"""
        workflow_template_yaml = os.path.expandvars(self.config["workflow_template_yaml"])
        butler_repo = dbi.get_repo(data.db_id)
        script_data = self.resolve_templated_strings(
            self.run_script_url_template_names,
            {},
            prod_base_url=dbi.get_prod_base(data.db_id),
            fullname=data.fullname,
        )
        outpath = script_data["config_url"]
        script = dbi.add_script(checker=self.yaml_checker_class, **script_data)
        import yaml

        with open(workflow_template_yaml, "rt", encoding="utf-8") as fin:
            workflow_config = yaml.safe_load(fin)

        workflow_config["project"] = data.p_name
        workflow_config["campaign"] = f"{data.p_name}/{data.c_name}"

        workflow_config["pipelineYaml"] = self.config["pipeline_yaml"][data.s_name]
        payload = dict(
            payloadName=f"{data.p_name}/{data.c_name}",
            output=data.coll_out,
            butlerConfig=butler_repo,
            inCollection=data.coll_in,
        )
        workflow_config["payload"] = payload
        with open(outpath, "wt", encoding="utf-8") as fout:
            yaml.dump(workflow_config, fout)
        return script

    def fake_run_hook(
        self,
        dbi: DbInterface,
        data,
        status: StatusEnum = StatusEnum.completed,
    ) -> None:
        script_id = data.run_script
        script = dbi.get_script(script_id)
        write_status_to_yaml(script.log_url, status)  # type: ignore

    def collect_script_hook(
        self, level: LevelEnum, dbi: DbInterface, itr: Iterable, data
    ) -> Optional[ScriptBase]:
        assert level.value >= LevelEnum.campaign.value
        if level == LevelEnum.campaign:
            return None
        butler_repo = dbi.get_repo(data.db_id)
        script_data = self.resolve_templated_strings(
            self.collect_script_url_template_names,
            {},
            prod_base_url=dbi.get_prod_base(data.db_id),
            fullname=data.fullname,
        )
        script = dbi.add_script(checker=self.yaml_checker_class, **script_data)
        with open(script.script_url, "wt", encoding="utf-8") as fout:
            fout.write(make_butler_chain_command(butler_repo, data, itr))
            fout.write("\n")
        write_status_to_yaml(script.log_url, StatusEnum.completed)
        return script

    def accept_hook(self, level: LevelEnum, dbi: DbInterface, itr: Iterable, data) -> None:
        return

    def reject_hook(self, level: LevelEnum, dbi: DbInterface, data) -> None:
        return
