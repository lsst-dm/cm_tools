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
from typing import Any, Iterable, Optional

from lsst.cm.tools.core.db_interface import DbId, DbInterface
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

    step_dict = OrderedDict(
        [("step1", ExampleStep1Grouper), ("step2", ExampleStep2Grouper), ("step3", ExampleStep3Grouper)]
    )

    yaml_checker_class = YamlChecker().get_checker_class_name()

    def prepare_script_hook(self, level: LevelEnum, dbi: DbInterface, db_id: DbId, data) -> Optional[int]:
        assert level.value >= LevelEnum.campaign.value
        if level == LevelEnum.workflow:
            return None
        butler_repo = dbi.get_repo(db_id)
        script_data = self._resolve_templated_strings(
            self.prepare_script_url_tempatle_names,
            {},
            prod_base_url=dbi.get_prod_base(db_id),
            fullname=data.fullname,
        )
        script_id = dbi.add_script(checker=self.yaml_checker_class, **script_data)
        with open(script_data['script_url'], "wt", encoding="utf-8") as fout:
            fout.write(make_butler_associate_command(butler_repo, data))
            fout.write("\n")
        write_status_to_yaml(script_data['log_url'], StatusEnum.completed)
        return script_id

    def workflow_hook(self, dbi: DbInterface, db_id: DbId, data, **kwargs) -> str:
        """Internal function to write the bps.yaml file for a given workflow"""
        workflow_template_yaml = os.path.expandvars(self.config["workflow_template_yaml"])
        butler_repo = dbi.get_repo(db_id)
        script_data = self._resolve_templated_strings(
            self.run_script_url_template_names,
            {},
            prod_base_url=dbi.get_prod_base(db_id),
            fullname=data.fullname,
        )
        outpath = script_data['config_url']
        script_id = dbi.add_script(checker="lsst.cm.tools.core.script_utils.YamlChecker", **script_data)
        tokens = data.fullname.split("/")
        production_name = tokens[0]
        campaign_name = tokens[1]
        step_name = tokens[2]
        import yaml

        with open(workflow_template_yaml, "rt", encoding="utf-8") as fin:
            workflow_config = yaml.safe_load(fin)

        workflow_config["project"] = production_name
        workflow_config["campaign"] = f"{production_name}/{campaign_name}"

        workflow_config["pipelineYaml"] = self.config["pipeline_yaml"][step_name]
        payload = dict(
            payloadName=f"{production_name}/{campaign_name}",
            output=data.coll_out,
            butlerConfig=butler_repo,
            inCollection=data.coll_in,
        )
        workflow_config["payload"] = payload
        with open(outpath, "wt", encoding="utf-8") as fout:
            yaml.dump(workflow_config, fout)
        return script_id

    def fake_run_hook(
        self, dbi: DbInterface, db_id: DbId, data, status: StatusEnum = StatusEnum.completed,
    ) -> None:
        script_id = data.run_script
        script_data = dbi.get_script(script_id)
        write_status_to_yaml(script_data.log_url, status)

    def collection_hook(
        self, level: LevelEnum, dbi: DbInterface, db_id: DbId, itr: Iterable, data
    ) -> dict[str, Any]:
        assert level.value >= LevelEnum.campaign.value
        if level == LevelEnum.campaign:
            return dict(status=StatusEnum.collecting, collect_script=None)
        butler_repo = dbi.get_repo(db_id)
        script_data = self._resolve_templated_strings(
            self.collect_script_url_template_names,
            {},
            prod_base_url=dbi.get_prod_base(db_id),
            fullname=data.fullname,
        )
        script_id = dbi.add_script(checker=self.yaml_checker_class, **script_data)
        with open(script_data['script_url'], "wt", encoding="utf-8") as fout:
            fout.write(make_butler_chain_command(butler_repo, data, itr))
            fout.write("\n")
        write_status_to_yaml(script_data['log_url'], StatusEnum.completed)
        return dict(status=StatusEnum.collecting, collect_script=script_id)

    def accept_hook(self, level: LevelEnum, dbi: DbInterface, db_id: DbId, itr: Iterable, data) -> None:
        return

    def reject_hook(self, level: LevelEnum, dbi: DbInterface, db_id: DbId, data) -> None:
        return
