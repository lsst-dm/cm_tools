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

from lsst.cm.tools.core.db_interface import DbId, DbInterface
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.tables import get_prefix, get_update_field_list

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
        prod_base_url="archive_test",
        coll_in_template="/prod/{basecoll}_input",
        coll_out_template="/prod/{basecoll}_output",
        prepare_script_url_template="{prod_base_url}/{fullname}/prepare.sh",
        prepare_log_url_template="{prod_base_url}/{fullname}/prepare.log",
        collect_script_url_template="{prod_base_url}/{fullname}/collect.sh",
        collect_log_url_template="{prod_base_url}/{fullname}/collect.log",
        command_tmpl="bps",
    )

    step_dict: OrderedDict[str, type] = OrderedDict([])

    @staticmethod
    def _check_unused(level: LevelEnum, dbi, parent_db_id, match_name):  # pylint: disable=unused-argument
        pass

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
        kwcopy["basecoll"] = kwcopy["fullname"]
        func_dict = {
            LevelEnum.production: self._get_insert_production_fields,
            LevelEnum.campaign: self._get_insert_campaign_fields,
            LevelEnum.step: self._get_insert_step_fields,
            LevelEnum.group: self._get_insert_group_fields,
            LevelEnum.workflow: self._get_insert_workflow_fields,
        }
        the_func = func_dict[level]
        ret_dict = the_func(dbi, parent_db_id, **kwcopy)
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
        field_list = get_update_field_list(level)
        status_fields = {
            LevelEnum.production: None,
            LevelEnum.campaign: "c_status",
            LevelEnum.step: "s_status",
            LevelEnum.group: "g_status",
            LevelEnum.workflow: "w_status",
        }
        status_field = status_fields[level]
        update_fields = self._copy_fields(field_list, **kwcopy)
        if kwcopy.get("status") is not None and status_field is not None:
            update_fields[status_field] = kwcopy.get("status")
        return update_fields

    def prepare_hook(
        self, level: LevelEnum, dbi: DbInterface, db_id: DbId, data, recurse: bool = True, **kwargs,
    ) -> None:
        path_var_names = {
            LevelEnum.production: "p_name",
            LevelEnum.campaign: "fullname",
            LevelEnum.step: "fullname",
            LevelEnum.group: "fullname",
            LevelEnum.workflow: "fullname",
        }
        if not self._check_prerequistes(level, dbi, db_id, data):
            return
        path_var_name = path_var_names[level]
        full_path = os.path.join(
            self._get_config_var("prod_base_url", "archive", **kwargs), data[path_var_name],
        )
        safe_makedirs(full_path)
        update_kwargs = dict(status=StatusEnum.ready)
        prefix = get_prefix(level)
        if level != LevelEnum.production:
            if data[f"{prefix}_prepare_script_url"] is not None:
                self.prepare_script_hook(level, dbi, db_id, data)
                update_kwargs["status"] = StatusEnum.preparing
        if level == LevelEnum.step:
            self._make_groups(dbi, db_id, data, recurse)
        elif level == LevelEnum.workflow:
            update_kwargs["workflow_tmpl_url"] = self._copy_workflow_template(data, **kwargs)
        dbi.update(level, db_id, **update_kwargs)

    def _get_insert_production_fields(self, dbi: DbInterface, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Production specific version of get_insert_fields()"""
        p_name = self._get_kwarg_value("production_name", **kwargs)
        self._check_unused(LevelEnum.production, dbi, parent_db_id, p_name)
        insert_fields = dict(p_name=p_name)
        return insert_fields

    def _get_insert_campaign_fields(self, dbi: DbInterface, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Campaign specific version of get_insert_fields()"""
        fullname = kwargs.get("fullname")
        self._check_unused(LevelEnum.campaign, dbi, parent_db_id, fullname)
        template_names = dict(
            c_coll_in="coll_in_template",
            c_coll_out="coll_out_template",
            c_prepare_script_url="prepare_script_url_template",
            c_prepare_log_url="prepare_log_url_template",
            c_collect_script_url="collect_script_url_template",
            c_collect_log_url="collect_log_url_template",
        )
        insert_fields = dict(
            fullname=fullname,
            c_name=self._get_kwarg_value("campaign_name", **kwargs),
            p_id=parent_db_id.p_id,
            c_data_query=self._get_config_var("c_data_query", "", **kwargs),
            c_coll_source=self._get_config_var("c_coll_source", "", **kwargs),
            c_status=StatusEnum.waiting,
        )
        extra_fields = self._resolve_templated_strings(template_names, insert_fields, **kwargs)
        insert_fields.update(**extra_fields)
        return insert_fields

    def _get_insert_step_fields(self, dbi: DbInterface, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Step specific version of get_insert_fields()"""
        previous_step_id = kwargs.get("previous_step_id")
        fullname = kwargs.get("fullname")
        self._check_unused(LevelEnum.step, dbi, parent_db_id, fullname)
        template_names = dict(
            s_coll_in="coll_in_template",
            s_coll_out="coll_out_template",
            s_prepare_script_url="prepare_script_url_template",
            s_prepare_log_url="prepare_log_url_template",
            s_collect_script_url="collect_script_url_template",
            s_collect_log_url="collect_log_url_template",
        )
        insert_fields = dict(
            fullname=fullname,
            s_name=self._get_kwarg_value("step_name", **kwargs),
            previous_step_id=previous_step_id,
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_data_query=self._get_config_var("s_data_query", "", **kwargs),
            s_coll_source=self._get_config_var("s_coll_source", "", **kwargs),
            s_status=StatusEnum.waiting,
        )
        extra_fields = self._resolve_templated_strings(template_names, insert_fields, **kwargs)
        insert_fields.update(**extra_fields)
        return insert_fields

    def _get_insert_group_fields(self, dbi: DbInterface, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Group specific version of get_insert_fields()"""
        fullname = kwargs.get("fullname")
        self._check_unused(LevelEnum.group, dbi, parent_db_id, fullname)
        template_names = dict(
            g_coll_in="coll_in_template",
            g_coll_out="coll_out_template",
            g_prepare_script_url="prepare_script_url_template",
            g_prepare_log_url="prepare_log_url_template",
            g_collect_script_url="collect_script_url_template",
            g_collect_log_url="collect_log_url_template",
        )
        insert_fields = dict(
            fullname=fullname,
            g_name=self._get_kwarg_value("group_name", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_id=parent_db_id.s_id,
            g_data_query=self._get_config_var("g_data_query", "", **kwargs),
            g_coll_source=self._get_config_var("g_coll_source", "", **kwargs),
            g_status=StatusEnum.waiting,
        )
        extra_fields = self._resolve_templated_strings(template_names, insert_fields, **kwargs)
        insert_fields.update(**extra_fields)
        return insert_fields

    def _get_insert_workflow_fields(self, dbi: DbInterface, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        """Workflow specific version of get_insert_fields()"""
        fullname = kwargs.get("fullname")
        w_run_log_url = os.path.join(
            self._get_config_var("prod_base_url", "archive", **kwargs), fullname, "workflow_log.yaml",
        )
        self._check_unused(LevelEnum.workflow, dbi, parent_db_id, fullname)
        template_names = dict(
            w_coll_in="coll_in_template",
            w_coll_out="coll_out_template",
            w_prepare_script_url="prepare_script_url_template",
            w_prepare_log_url="prepare_log_url_template",
            w_collect_script_url="collect_script_url_template",
            w_collect_log_url="collect_log_url_template",
        )
        insert_fields = dict(
            fullname=fullname,
            w_idx=self._get_kwarg_value("workflow_idx", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_id=parent_db_id.s_id,
            g_id=parent_db_id.g_id,
            w_run_script_url=self._get_config_var("command_tmpl", "", **kwargs),
            w_run_log_url=w_run_log_url,
            w_coll_source=self._get_config_var("w_coll_source", "", **kwargs),
            w_status=StatusEnum.waiting,
        )
        extra_fields = dict(w_data_query=self._get_data_query(dbi, insert_fields, **kwargs))
        extra_fields.update(self._resolve_templated_strings(template_names, insert_fields, **kwargs))
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
        s_coll_source = insert_fields.get("c_coll_in")
        parent_db_id = dbi.get_db_id(LevelEnum.campaign, **kwcopy)
        for step_name in self.step_dict.keys():
            kwcopy.update(step_name=step_name)
            kwcopy.update(previous_step_id=previous_step_id)
            kwcopy.update(s_coll_source=s_coll_source)
            step_insert = dbi.insert(LevelEnum.step, parent_db_id, self, recurse, **kwcopy)
            s_coll_source = step_insert.get("s_coll_out")
            previous_step_id = dbi.get_row_id(LevelEnum.step, **kwcopy)

    def _post_insert_group(
        self, dbi: DbInterface, insert_fields: dict[str, Any], recurse: bool = True, **kwargs,
    ) -> None:
        """Group specific version of post_insert_hook()"""
        kwcopy = kwargs.copy()
        kwcopy["workflow_idx"] = kwcopy.get("n_workflows", 0)
        g_coll_in = insert_fields.get("g_coll_in")
        kwcopy.update(w_coll_source=g_coll_in)
        parent_db_id = dbi.get_db_id(LevelEnum.group, **kwcopy)
        dbi.insert(LevelEnum.workflow, parent_db_id, self, recurse, **kwcopy)
        if recurse:
            dbi.prepare(LevelEnum.workflow, parent_db_id, recurse)

    def launch_workflow_hook(self, dbi: DbInterface, db_id: DbId, data):
        w_run_script_url = data["w_run_script_url"]
        workflow_tmpl_url = data["workflow_tmpl_url"]
        submit_command = f"{w_run_script_url} {workflow_tmpl_url}"
        # workflow_start = datetime.now()
        print(f"Submitting workflow {str(db_id)} with {submit_command}")
        update_fields = dict(
            workflow_subm_url=workflow_tmpl_url, w_run_script_url=w_run_script_url, status=StatusEnum.running,
        )
        dbi.update(LevelEnum.workflow, db_id, **update_fields)

    def _group_iterator(self, dbi: DbInterface, parent_data_id: DbId, data, **kwargs) -> Iterable:
        step_name = str(kwargs.get("step_name"))
        try:
            grouper_class = self.step_dict[step_name]
            grouper = grouper_class()
        except KeyError as msg:  # pragma: no cover
            raise KeyError(f"No Grouper object associated to step {step_name}") from msg
        return grouper(self.config, dbi, parent_data_id, data, **kwargs)

    def _make_groups(self, dbi: DbInterface, db_id: DbId, data, recurse: bool = True) -> None:
        """Internal function called to insert groups into a given step"""
        tokens = data["fullname"].split("/")
        insert_fields = dict(
            production_name=tokens[0],
            campaign_name=tokens[1],
            step_name=tokens[2],
            g_coll_source=data["s_coll_in"],
        )
        for group_kwargs in self._group_iterator(dbi, db_id, data, **insert_fields):
            insert_fields.update(**group_kwargs)
            dbi.insert(LevelEnum.group, db_id, self, recurse, **insert_fields)
        if recurse:
            dbi.prepare(LevelEnum.group, db_id, recurse)

    def _copy_workflow_template(self, data, **kwargs) -> str:
        """Internal function to write the bps.yaml file for a given workflow"""
        workflow_template_yaml = os.path.expandvars(self.config["workflow_template_yaml"])
        with open(workflow_template_yaml, "rt", encoding="utf-8") as fin:
            lines = fin.readlines()
        outpath = os.path.join(
            self._get_config_var("prod_base_url", "archive", **kwargs), data["fullname"], "bps.yaml",
        )

        step_name = data["fullname"].split("/")[2]
        format_vars = dict(
            w_coll_in=data["w_coll_in"],
            w_coll_out=data["w_coll_out"],
            butler_config=self.config["bulter_config"],
            data_query=data["w_data_query"],
            pipeline_yaml=self.config["pipeline_yaml"][step_name],
            sw_image=self.config["sw_image"],
        )

        with open(outpath, "wt", encoding="utf-8") as fout:
            for line_ in lines:
                fout.write(line_.format(**format_vars))
        return outpath

    def _check_prerequistes(self, level: LevelEnum, dbi: DbInterface, db_id: DbId, data) -> bool:
        """Internal function to see if the pre-requistes for a given step
        have been completed"""
        if level in [
            LevelEnum.production,
            LevelEnum.campaign,
            LevelEnum.group,
            LevelEnum.workflow,
        ]:
            return True
        previous_step_id = data["previous_step_id"]
        if previous_step_id is None:
            return True
        parent_db_id = DbId(p_id=db_id.p_id, c_id=db_id.c_id, s_id=previous_step_id)
        status = dbi.get_status(LevelEnum.step, parent_db_id)
        if status == StatusEnum.accepted:
            return True
        return False
