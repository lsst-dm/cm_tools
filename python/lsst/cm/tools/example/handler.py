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

from collections import OrderedDict
from typing import Any, Iterable

from lsst.cm.tools.core.db_interface import DbId, DbInterface
from lsst.cm.tools.core.grouper import Grouper
from lsst.cm.tools.core.script_utils import make_butler_associate_command, make_butler_chain_command
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, check_status_from_yaml, write_status_to_yaml
from lsst.cm.tools.db.sqlalch_handler import SQLAlchemyHandler


class ExampleStep1Grouper(Grouper):
    def _do_call(self):
        out_dict = dict(
            production_name=self.config["production_name"],
            campaign_name=self.config["campaign_name"],
            step_name=self.config["step_name"],
        )

        for i in range(10):
            out_dict.update(group_name=f"group_{i}", g_data_query=f"i == {i}")
            yield out_dict


class ExampleStep2Grouper(Grouper):
    def _do_call(self):
        out_dict = dict(
            production_name=self.config["production_name"],
            campaign_name=self.config["campaign_name"],
            step_name=self.config["step_name"],
        )

        for i in range(20):
            out_dict.update(group_name=f"group_{i}", g_data_query=f"i == {i}")
            yield out_dict


class ExampleStep3Grouper(Grouper):
    def _do_call(self):
        out_dict = dict(
            production_name=self.config["production_name"],
            campaign_name=self.config["campaign_name"],
            step_name=self.config["step_name"],
        )

        for i in range(20):
            out_dict.update(group_name=f"group_{i}", g_data_query=f"i == {i}")
            yield out_dict


class ExampleHandler(SQLAlchemyHandler):

    step_dict = OrderedDict(
        [("step1", ExampleStep1Grouper), ("step2", ExampleStep2Grouper), ("step3", ExampleStep3Grouper)]
    )

    def prepare_script_hook(self, level: LevelEnum, dbi: DbInterface, db_id: DbId, data,) -> None:
        butler_repo = self.config["butler_repo"]
        prepare_script_url = data["prepare_script_url"]
        with open(prepare_script_url, "wt", encoding="utf-8") as fout:
            fout.write(make_butler_associate_command(butler_repo, data))
            fout.write('\n')
        write_status_to_yaml(data["prepare_log_url"], StatusEnum.completed)

    def check_workflow_status_hook(self, dbi: DbInterface, db_id: DbId, data) -> dict[str, Any]:
        return dict(status=check_status_from_yaml(data["run_log_url"], data["status"]))

    def fake_run_hook(
        self, dbi: DbInterface, db_id: DbId, data, status: StatusEnum = StatusEnum.completed,
    ) -> None:
        write_status_to_yaml(data["run_log_url"], status)

    def collection_hook(
        self, level: LevelEnum, dbi: DbInterface, db_id: DbId, itr: Iterable, data
    ) -> StatusEnum:
        butler_repo = self.config["butler_repo"]
        collect_script_url = data["collect_script_url"]
        with open(collect_script_url, "wt", encoding="utf-8") as fout:
            fout.write(make_butler_chain_command(butler_repo, data, itr))
            fout.write('\n')
        write_status_to_yaml(data["collect_log_url"], StatusEnum.completed)
        return StatusEnum.collecting

    def check_script_status_hook(self, log_url) -> StatusEnum:
        return check_status_from_yaml(log_url, StatusEnum.running)

    def accept_hook(self, level: LevelEnum, dbi: DbInterface, db_id: DbId, itr: Iterable, data) -> None:
        return

    def reject_hook(self, level: LevelEnum, dbi: DbInterface, db_id: DbId, data) -> None:
        return
