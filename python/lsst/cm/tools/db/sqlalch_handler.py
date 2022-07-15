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
from typing import Any, Iterable

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import db

# import datetime


def safe_makedirs(path):
    try:
        os.makedirs(path)
    except OSError:
        pass


class SQLAlchemyHandler(Handler):  # noqa
    """SQLAlchemy based Handler

    This contains the implementation details that
    are specific to the SQLAlchemy based DB struture.
    """

    default_config = dict(
        coll_in_template="/prod/{fullname}_input",
        coll_out_template="/prod/{fullname}_output",
        prepare_script_url_template="{prod_base_url}/{fullname}/prepare.sh",
        prepare_log_url_template="{prod_base_url}/{fullname}/prepare.log",
        collect_script_url_template="{prod_base_url}/{fullname}/collect.sh",
        collect_log_url_template="{prod_base_url}/{fullname}/collect.log",
        run_script_url_template="bps",
        run_log_url_template="{prod_base_url}/{fullname}/run.log",
        run_config_url_template="{prod_base_url}/{fullname}/bps.yaml",
    )

    coll_template_names = dict(coll_in="coll_in_template", coll_out="coll_out_template",)

    prepare_script_url_tempatle_names = dict(
        script_url="prepare_script_url_template", log_url="prepare_log_url_template",
    )

    collect_script_url_template_names = dict(
        script_url="collect_script_url_template", log_url="collect_log_url_template",
    )

    run_script_url_template_names = dict(
        script_url="run_script_url_template",
        log_url="run_log_url_template",
        config_url="run_config_url_template",
    )

    step_dict: OrderedDict[str, type] = OrderedDict([])

    @staticmethod
    def _copy_fields(fields: list[str], **kwargs) -> dict[str, Any]:
        ret_dict = {}
        for field_ in fields:
            if field_ in kwargs:
                ret_dict[field_] = kwargs.get(field_)
        return ret_dict

    def get_insert_fields_hook(
        self, level: LevelEnum, dbi: DbInterface, parent_db_id: DbId, **kwargs
    ) -> dict[str, Any]:
        kwcopy = kwargs.copy()
        kwcopy["fullname"] = dbi.full_name(level, **kwargs)
        if level.value > LevelEnum.campaign.value:
            kwcopy["prod_base_url"] = dbi.get_prod_base(parent_db_id)
        elif level.value == LevelEnum.campaign.value:
            if "butler_repo" not in kwcopy:
                raise KeyError("butler_repo must be specified with inserting a campaign")
            if "prod_base_url" not in kwcopy:
                raise KeyError("prod_base_url must be specified with inserting a campaign")
        func_dict = {
            LevelEnum.campaign: self._get_insert_campaign_fields,
            LevelEnum.step: self._get_insert_step_fields,
            LevelEnum.group: self._get_insert_group_fields,
        }
        if level == LevelEnum.production:
            ret_dict = self._get_insert_production_fields(**kwcopy)
        elif level in [LevelEnum.campaign, LevelEnum.step, LevelEnum.group]:
            the_func = func_dict[level]
            ret_dict = the_func(parent_db_id, **kwcopy)
        else:
            ret_dict = self._get_insert_workflow_fields(dbi, parent_db_id, **kwcopy)
        ret_dict["handler"] = self.get_handler_class_name()
        ret_dict["config_yaml"] = self._config_url
        return ret_dict

    def post_insert_hook(
        self,
        level: LevelEnum,
        dbi: DbInterface,
        insert_fields: dict[str, Any],
        recurse: bool = False,
        **kwargs,
    ) -> None:
        func_dict = {
            LevelEnum.production: None,
            LevelEnum.campaign: self._post_insert_campaign,
            LevelEnum.step: None,
            LevelEnum.group: self._post_insert_group,
            LevelEnum.workflow: None,
        }
        the_func = func_dict[level]
        if the_func is None:
            return
        the_func(dbi, insert_fields, recurse, **kwargs)

    def get_update_fields_hook(
        self, level: LevelEnum, dbi: DbInterface, data, itr: Iterable, **kwargs
    ) -> dict[str, Any]:
        kwcopy = kwargs.copy()
        field_list = db.get_update_field_list(level)
        status_fields = {
            LevelEnum.production: None,
            LevelEnum.campaign: "status",
            LevelEnum.step: "status",
            LevelEnum.group: "status",
            LevelEnum.workflow: "status",
        }
        status_field = status_fields[level]
        update_fields = self._copy_fields(field_list, **kwcopy)
        if kwcopy.get("status") is not None and status_field is not None:
            update_fields[status_field] = kwcopy.get("status")
        return update_fields

    def prepare_hook(
        self, level: LevelEnum, dbi: DbInterface, db_id: DbId, data, recurse: bool = True, **kwargs,
    ) -> list[DbId]:
        path_var_names = {
            LevelEnum.production: "p_name",
            LevelEnum.campaign: "fullname",
            LevelEnum.step: "fullname",
            LevelEnum.group: "fullname",
            LevelEnum.workflow: "fullname",
        }
        db_id_list = []
        if not self._check_prerequistes(level, dbi, db_id):
            return db_id_list
        path_var_name = path_var_names[level]
        prod_base_url = dbi.get_prod_base(db_id)
        full_path = os.path.join(prod_base_url, data.__dict__[path_var_name])
        safe_makedirs(full_path)
        update_kwargs = {}
        db_id_list.append(db_id)
        script_id = self.prepare_script_hook(level, dbi, db_id, data)
        if script_id is not None:
            update_kwargs["prepare_script"] = script_id
        if level != LevelEnum.production:
            update_kwargs["status"] = StatusEnum.preparing
        if level == LevelEnum.step:
            db_id_list += self._make_groups(dbi, db_id, data, recurse)
        elif level == LevelEnum.workflow:
            update_kwargs["run_script"] = self.workflow_hook(dbi, db_id, data, **kwargs)
        dbi.update(level, db_id, **update_kwargs)
        return db_id_list

    def _get_insert_production_fields(self, **kwargs) -> dict[str, Any]:
        """Production specific version of get_insert_fields()"""
        p_name = self._get_kwarg_value("production_name", **kwargs)
        insert_fields = dict(p_name=p_name)
        return insert_fields

    def _get_insert_campaign_fields(self, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Campaign specific version of get_insert_fields()"""
        fullname = kwargs.get("fullname")
        insert_fields = dict(
            fullname=fullname,
            c_name=self._get_kwarg_value("campaign_name", **kwargs),
            p_id=parent_db_id.p_id,
            data_query=self._get_config_var("data_query", "", **kwargs),
            coll_source=self._get_config_var("coll_source", "", **kwargs),
            status=StatusEnum.waiting,
            butler_repo=kwargs["butler_repo"],
            prod_base_url=kwargs["prod_base_url"],
        )
        extra_fields = self._resolve_templated_strings(self.coll_template_names, insert_fields, **kwargs)
        insert_fields.update(**extra_fields)
        return insert_fields

    def _get_insert_step_fields(self, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Step specific version of get_insert_fields()"""
        previous_step_id = kwargs.get("previous_step_id")
        fullname = kwargs.get("fullname")
        insert_fields = dict(
            fullname=fullname,
            s_name=self._get_kwarg_value("step_name", **kwargs),
            previous_step_id=previous_step_id,
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            data_query=self._get_config_var("data_query", "", **kwargs),
            coll_source=self._get_config_var("coll_source", "", **kwargs),
            status=StatusEnum.waiting,
        )
        extra_fields = self._resolve_templated_strings(self.coll_template_names, insert_fields, **kwargs)
        insert_fields.update(**extra_fields)
        return insert_fields

    def _get_insert_group_fields(self, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Group specific version of get_insert_fields()"""
        fullname = kwargs.get("fullname")
        insert_fields = dict(
            fullname=fullname,
            g_name=self._get_kwarg_value("group_name", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_id=parent_db_id.s_id,
            data_query=self._get_config_var("data_query", "", **kwargs),
            coll_source=self._get_config_var("coll_source", "", **kwargs),
            status=StatusEnum.waiting,
        )
        extra_fields = self._resolve_templated_strings(self.coll_template_names, insert_fields, **kwargs)
        insert_fields.update(**extra_fields)
        return insert_fields

    def _get_insert_workflow_fields(self, dbi: DbInterface, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Workflow specific version of get_insert_fields()"""
        fullname = kwargs.get("fullname")
        insert_fields = dict(
            fullname=fullname,
            w_idx=self._get_kwarg_value("workflow_idx", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_id=parent_db_id.s_id,
            g_id=parent_db_id.g_id,
            coll_source=self._get_config_var("coll_source", "", **kwargs),
            status=StatusEnum.waiting,
        )
        extra_fields = dict(data_query=self._get_data_query(dbi, insert_fields, **kwargs))
        extra_fields.update(
            self._resolve_templated_strings(self.coll_template_names, insert_fields, **kwargs)
        )
        insert_fields.update(**extra_fields)
        return insert_fields

    def _get_data_query(self, dbi, insert_fields, **kwcopy):  # pylint: disable=unused-argument
        return kwcopy.get("data_query")

    def _post_insert_campaign(
        self, dbi: DbInterface, insert_fields: dict[str, Any], recurse: bool = True, **kwargs,
    ) -> None:
        """Campaign specific version of post_insert_hook()"""
        kwcopy = kwargs.copy()
        previous_step_id = None
        coll_source = insert_fields.get("coll_in")
        parent_db_id = dbi.get_db_id(LevelEnum.campaign, **kwcopy)
        for step_name in self.step_dict.keys():
            kwcopy.update(step_name=step_name)
            kwcopy.update(previous_step_id=previous_step_id)
            kwcopy.update(coll_source=coll_source)
            step_insert = dbi.insert(LevelEnum.step, parent_db_id, self, recurse, **kwcopy)
            step_id = parent_db_id.extend(LevelEnum.step, step_insert["s_id"])
            coll_source = step_insert.get("coll_out")
            if previous_step_id is not None:
                dbi.add_prerequisite(step_id, parent_db_id.extend(LevelEnum.step, previous_step_id))
            previous_step_id = dbi.get_row_id(LevelEnum.step, **kwcopy)

    def _post_insert_group(
        self, dbi: DbInterface, insert_fields: dict[str, Any], recurse: bool = True, **kwargs,
    ) -> None:
        """Group specific version of post_insert_hook()"""
        kwcopy = kwargs.copy()
        kwcopy["workflow_idx"] = kwcopy.get("n_child", 0)
        coll_in = insert_fields.get("coll_in")
        kwcopy.update(coll_source=coll_in)
        parent_db_id = dbi.get_db_id(LevelEnum.group, **kwcopy)
        dbi.insert(LevelEnum.workflow, parent_db_id, self, recurse, **kwcopy)
        if recurse:
            dbi.prepare(LevelEnum.workflow, parent_db_id, recurse)

    def launch_workflow_hook(self, dbi: DbInterface, db_id: DbId, data):
        script_id = data.run_script
        script_data = dbi.get_script(script_id)
        config_url = script_data.config_url
        script_url = script_data.script_url
        submit_command = f"{script_url} {config_url}"
        # workflow_start = datetime.now()
        print(f"Submitting workflow {str(db_id)} with {submit_command}")
        update_fields = dict(status=StatusEnum.running)
        dbi.update(LevelEnum.workflow, db_id, **update_fields)

    def _group_iterator(self, dbi: DbInterface, parent_data_id: DbId, data, **kwargs) -> Iterable:
        step_name = str(kwargs.get("step_name"))
        try:
            grouper_class = self.step_dict[step_name]
            grouper = grouper_class()
        except KeyError as msg:  # pragma: no cover
            raise KeyError(f"No Grouper object associated to step {step_name}") from msg
        return grouper(self.config, dbi, parent_data_id, data, **kwargs)

    def _make_groups(self, dbi: DbInterface, db_id: DbId, data, recurse: bool = True) -> list[DbId]:
        """Internal function called to insert groups into a given step"""
        tokens = data.fullname.split("/")
        insert_fields = dict(
            production_name=tokens[0],
            campaign_name=tokens[1],
            step_name=tokens[2],
            coll_source=data.coll_in,
        )
        db_id_list = []
        for group_kwargs in self._group_iterator(dbi, db_id, data, **insert_fields):
            insert_fields.update(**group_kwargs)
            dbi.insert(LevelEnum.group, db_id, self, recurse, **insert_fields)
        if recurse:
            db_id_list += dbi.prepare(LevelEnum.group, db_id, recurse)
        return db_id_list

    def _check_prerequistes(self, level: LevelEnum, dbi: DbInterface, db_id) -> bool:
        """Internal function to see if the pre-requistes for a given step
        have been completed"""
        prereq_list = dbi.get_prerequisites(level, db_id)
        for prereq_ in prereq_list:
            status = dbi.get_status(prereq_.level(), prereq_)
            if status.value < StatusEnum.accepted.value:
                return False
        return True
