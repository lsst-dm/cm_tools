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

from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase
from lsst.cm.tools.core.handler import ScriptHandlerBase
from lsst.cm.tools.core.script_utils import (
    FakeRollback,
    YamlChecker,
    make_butler_associate_command,
    make_butler_chain_command,
    make_validate_command,
    write_command_script,
    write_status_to_yaml,
)
from lsst.cm.tools.core.utils import LevelEnum, ScriptMethod, ScriptType, StatusEnum
from lsst.cm.tools.db.script import Script


class ScriptHandler(ScriptHandlerBase):

    default_config = dict(
        script_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
        stamp_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
        log_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
        config_url_template="{prod_base_url}/{fullname}/{name}_{idx:03}_bps.yaml",
    )

    script_url_template_names = dict(
        script_url="script_url_template",
        log_url="log_url_template",
        stamp_url="stamp_url_template",
    )

    script_type: ScriptType = ScriptType.prepare
    script_method = ScriptMethod.bash_stamp
    checker_class_name = YamlChecker().get_checker_class_name()
    rollback_class_name = FakeRollback().get_rollback_class_name()

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> ScriptBase:
        kwcopy = kwargs.copy()
        name = kwcopy.pop("name")
        idx = kwcopy.pop("idx")
        insert_fields = dict(
            name=name,
            idx=idx,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
            checker=self.checker_class_name,
            rollback=self.rollback_class_name,
            status=StatusEnum.ready,
            script_type=self.script_type,
            script_method=self.script_method,
            level=parent.level,
        )
        insert_fields.update(coll_out=self.get_coll_out_name(parent, **insert_fields))
        if parent.level == LevelEnum.campaign:
            insert_fields.update(c_id=parent.db_id.c_id)
        elif parent.level == LevelEnum.step:
            insert_fields.update(s_id=parent.db_id.s_id)
        elif parent.level == LevelEnum.group:
            insert_fields.update(g_id=parent.db_id.g_id)
        script_data = self.resolve_templated_strings(
            self.script_url_template_names,
            prod_base_url=parent.prod_base_url,
            fullname=parent.fullname,
            idx=idx,
            name=name,
        )
        insert_fields.update(**script_data)
        script = Script.insert_values(dbi, **insert_fields)
        fake_run = kwcopy.pop("fake_run")
        if fake_run:
            self.fake_run_hook(dbi, script, fake_run)
        self.write_script_hook(dbi, parent, script, **kwcopy)
        return script

    def fake_run_hook(
        self, dbi: DbInterface, script: ScriptBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        write_status_to_yaml(script.log_url, status)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        raise NotImplementedError()


class PrepareScriptHandler(ScriptHandler):

    script_type: ScriptType = ScriptType.prepare

    def write_script_hook(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> None:
        command = make_butler_associate_command(parent.butler_repo, parent)
        write_command_script(script, command, **kwargs)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        return parent.coll_in


class CollectScriptHandler(ScriptHandler):

    script_type: ScriptType = ScriptType.collect

    def write_script_hook(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> None:
        command = make_butler_chain_command(parent.butler_repo, parent)
        write_command_script(script, command, **kwargs)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        return parent.coll_out


class ValidateScriptHandler(ScriptHandler):

    script_type: ScriptType = ScriptType.validate

    def write_script_hook(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> None:
        command = make_validate_command(parent.butler_repo, parent)
        write_command_script(script, command, **kwargs)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        return parent.coll_validate
