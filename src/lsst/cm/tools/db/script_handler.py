import os
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
from lsst.cm.tools.core.slurm_utils import SlurmChecker, submit_job
from lsst.cm.tools.core.utils import LevelEnum, ScriptMethod, ScriptType, StatusEnum
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.script import Script


class ScriptHandler(ScriptHandlerBase):
    """Script callback handler

    Provides interface functions.

    Derived classes will have to:

    1. define the `script_type`
    2. implement `write_script_hook` to write the script to run
    3. implement `get_coll_out_name` to get the script output collection name
    """

    default_config = dict(
        templates=dict(
            script_url="{prod_base_url}/{fullname}/{name}_{idx:03}.sh",
            stamp_url="{prod_base_url}/{fullname}/{name}_{idx:03}.stamp",
            log_url="{prod_base_url}/{fullname}/{name}_{idx:03}.log",
        )
    )

    script_type: ScriptType = ScriptType.prepare
    checker_class_dict = {
        ScriptMethod.no_script: None,
        ScriptMethod.bash: YamlChecker,
        ScriptMethod.slurm: SlurmChecker,
    }
    rollback_class_name = FakeRollback().get_rollback_class_name()

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> ScriptBase:
        kwcopy = kwargs.copy()
        name = kwcopy.pop("name")
        prev_scripts = [script for script in parent.scripts_ if script.name == name]
        idx = len(prev_scripts)
        checker_class = self.checker_class_dict[self.script_method]
        if checker_class is None:
            checker_class_name = None
        else:
            checker_class_name = checker_class().get_checker_class_name()
        insert_fields = dict(
            name=name,
            idx=idx,
            frag_id=self._fragment_id,
            checker=checker_class_name,
            rollback=self.rollback_class_name,
            status=StatusEnum.ready,
            script_type=self.script_type,
            script_method=self.script_method,
            level=parent.level,
        )
        insert_fields.update(coll_out=self.get_coll_out_name(parent, **insert_fields))
        if parent.level == LevelEnum.campaign:
            insert_fields.update(
                c_id=parent.db_id.c_id,
            )
        elif parent.level == LevelEnum.step:
            insert_fields.update(
                c_id=parent.db_id.c_id,
                s_id=parent.db_id.s_id,
            )
        elif parent.level == LevelEnum.group:
            insert_fields.update(
                c_id=parent.db_id.c_id,
                s_id=parent.db_id.s_id,
                g_id=parent.db_id.g_id,
            )
        elif parent.level == LevelEnum.workflow:
            insert_fields.update(
                c_id=parent.db_id.c_id,
                s_id=parent.db_id.s_id,
                g_id=parent.db_id.g_id,
                w_id=parent.db_id.w_id,
            )
        script_data = self.resolve_templated_strings(
            prod_base_url=parent.prod_base_url,
            fullname=parent.fullname,
            idx=idx,
            name=name,
        )
        if self.script_method == ScriptMethod.slurm:
            script_data.pop("stamp_url")
        insert_fields.update(**script_data)
        script = Script.insert_values(dbi, **insert_fields)
        return script

    def fake_run_hook(
        self, dbi: DbInterface, script: ScriptBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        write_status_to_yaml(script.stamp_url, status)

    def run(
        self,
        dbi: DbInterface,
        parent: Any,
        script: ScriptBase,
        **kwargs: Any,
    ) -> StatusEnum:
        if self.no_submit:  # pragma: no cover
            return StatusEnum.running
        self.write_script_hook(dbi, parent, script, **kwargs)
        if script.script_method == ScriptMethod.bash:
            os.system(f"source {script.script_url}")
        elif script.script_method == ScriptMethod.slurm:
            job_id = submit_job(script.script_url)
            Script.update_values(dbi, script.id, stamp_url=job_id)
        status = StatusEnum.running
        return status

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        """Get the name of the output collection of this script

        Parameters
        ----------
        parent : Any
            The parent entry for the script

        Returns
        -------
        coll_out : str
            The name of the output collection
        """
        raise NotImplementedError()


class PrepareScriptHandler(ScriptHandler):
    """Script handler for scripts that prepare input collections"""

    script_type: ScriptType = ScriptType.prepare

    def write_script_hook(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> None:
        command = make_butler_associate_command(
            parent.butler_repo,
            parent.coll_in,
            parent.coll_source,
            parent.data_query,
        )
        write_command_script(script, command, **kwargs)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        return parent.coll_in


class CollectScriptHandler(ScriptHandler):
    """Script handler for scripts that collect output collections"""

    script_type: ScriptType = ScriptType.collect

    def write_script_hook(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> None:
        input_colls = [child.coll_out for child in parent.children()]
        command = make_butler_chain_command(parent.butler_repo, parent.coll_out, input_colls)
        write_command_script(script, command, **kwargs)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        return parent.coll_out


class ValidateScriptHandler(ScriptHandler):
    """Script handler for scripts that run validate on output collections"""

    script_type: ScriptType = ScriptType.validate

    def write_script_hook(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> None:
        command = make_validate_command(parent.butler_repo, parent.coll_validate, parent.coll_out)
        write_command_script(script, command, **kwargs)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        return parent.coll_validate


class AncillaryScriptHandler(ScriptHandler):
    """Script handler for scripts that collect output collections"""

    config_block = "ancil"

    script_type: ScriptType = ScriptType.prepare

    def write_script_hook(
        self, dbi: DbInterface, parent: Campaign, script: ScriptBase, **kwargs: Any
    ) -> None:
        input_colls = self.config["collections"]
        command = make_butler_chain_command(parent.butler_repo, parent.coll_ancil, input_colls)
        write_command_script(script, command, **kwargs)

    def get_coll_out_name(self, parent: Any, **kwargs: Any) -> str:
        return parent.coll_ancil
