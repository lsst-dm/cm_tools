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

from collections.abc import Iterable

from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum


class DbInterface:

    @staticmethod
    def full_campaign_name(
            production_name: str,
            campaign_name: str) -> str:
        return f"{production_name}/{campaign_name}"

    @staticmethod
    def full_step_name(
            production_name: str,
            campaign_name: str,
            step_name: str) -> str:
        return f"{production_name}/{campaign_name}/{step_name}"

    @staticmethod
    def full_group_name(
            production_name: str,
            campaign_name: str,
            step_name: str,
            group_name: str) -> str:
        return f"{production_name}/{campaign_name}/{step_name}/{group_name}"

    @staticmethod
    def full_workflow_name(
            production_name: str,
            campaign_name: str,
            step_name: str,
            group_name: str,
            workflow_idx: int) -> str:
        return f"{production_name}/{campaign_name}/{step_name}/{group_name}/{workflow_idx:06}"

    @staticmethod
    def get_handler(handler_name: str):
        if handler_name is None:
            return None
        return Handler.create(handler_name)

    def invoke_handler_create(self, level: LevelEnum, ins_values, **kwargs):
        handler = self.get_handler(kwargs.get('handler'))
        out_values = ins_values.copy()
        out_values.update(**kwargs)
        if handler is not None:
            out_values = handler.insert(level, self, **out_values)
        return out_values

    def create_production(self, production_name: str, **kwargs):
        ins_values = dict(
            p_name=production_name)
        ins_values = self.invoke_handler_create(LevelEnum.production, ins_values, **kwargs)
        self._insert_production(**ins_values)
        self.print_productions()

    def create_campaign(self, production_name: str, campaign_name: str, **kwargs):
        p_id = self.get_production_id(production_name)
        fullname = self.full_campaign_name(production_name, campaign_name)
        ins_values = dict(
            fullname=fullname,
            c_name=campaign_name,
            p_id=p_id)
        ins_values = self.invoke_handler_create(LevelEnum.campaign, ins_values, **kwargs)
        self._insert_campaign(**ins_values)
        self.print_campaigns(p_id)

    def create_step(self, production_name: str, campaign_name: str, step_name: str, **kwargs):
        p_id = self.get_production_id(production_name)
        c_id = self.get_campaign_id(p_id, campaign_name)
        fullname = self.full_step_name(production_name, campaign_name, step_name)
        ins_values = dict(
            fullname=fullname,
            s_name=step_name,
            p_id=p_id,
            c_id=c_id)
        ins_values = self.invoke_handler_create(LevelEnum.step, ins_values, **kwargs)
        self._insert_step(**ins_values)
        self.print_steps(c_id)

    def create_group(
            self,
            production_name: str,
            campaign_name: str,
            step_name: str,
            group_name: str,
            **kwargs):
        p_id = self.get_production_id(production_name)
        c_id = self.get_campaign_id(p_id, campaign_name)
        s_id = self.get_step_id(c_id, step_name)
        fullname = self.full_group_name(production_name, campaign_name, step_name, group_name)
        ins_values = dict(
            fullname=fullname,
            g_name=group_name,
            p_id=p_id,
            c_id=c_id,
            s_id=s_id)
        ins_values = self.invoke_handler_create(LevelEnum.group, ins_values, **kwargs)
        self._insert_group(**ins_values)
        self.print_groups(s_id)

    def create_workflow(
            self,
            production_name: str,
            campaign_name: str,
            step_name: str,
            group_name: str,
            workflow_idx: int,
            **kwargs):
        p_id = self.get_production_id(production_name)
        c_id = self.get_campaign_id(p_id, campaign_name)
        s_id = self.get_step_id(c_id, step_name)
        g_id = self.get_group_id(s_id, group_name)
        fullname = self.full_workflow_name(
            production_name,
            campaign_name,
            step_name,
            group_name,
            workflow_idx)
        ins_values = dict(
            fullname=fullname,
            p_id=p_id,
            c_id=c_id,
            s_id=s_id,
            g_id=g_id,
            w_idx=workflow_idx)
        ins_values = self.invoke_handler_create(LevelEnum.group, ins_values, **kwargs)
        self._insert_workflow(**ins_values)
        self.print_workflows(g_id)

    def create(self, level, **kwargs):
        func_dict = {
            LevelEnum.production: self.create_production,
            LevelEnum.campaign: self.create_campaign,
            LevelEnum.step: self.create_step,
            LevelEnum.group: self.create_group,
            LevelEnum.workflow: self.create_workflow}
        the_func = func_dict[level]
        return the_func(**kwargs)

    def update(self, level, **kwargs):
        func_dict = {
            LevelEnum.production: self.update_production,
            LevelEnum.campaign: self.update_campaign,
            LevelEnum.step: self.update_step,
            LevelEnum.group: self.update_group,
            LevelEnum.workflow: self.update_workflow}
        the_func = func_dict[level]
        return the_func(**kwargs)

    def print_(self, level, row_id):
        print(level, row_id)
        if level == LevelEnum.production:
            self.print_productions()
        elif level == LevelEnum.campaign:
            self.print_campaigns(row_id)
        elif level == LevelEnum.step:
            self.print_steps(row_id)
        elif level == LevelEnum.group:
            self.print_groups(row_id)
        elif level == LevelEnum.workflow:
            self.print_workflows(row_id)
        return

    def count(self, level, row_id):
        if level == LevelEnum.production:
            print(self.count_productions())
        elif level == LevelEnum.campaign:
            print(self.count_campaigns(row_id))
        elif level == LevelEnum.step:
            print(self.count_steps(row_id))
        elif level == LevelEnum.group:
            print(self.count_groups(row_id))
        elif level == LevelEnum.workflow:
            print(self.count_workflows(row_id))
        return

    def update_production(
            self,
            production_name: str,
            **kwargs):
        p_id = self.get_production_id(production_name)
        self._update_production(p_id, **kwargs)

    def update_campaign(
            self,
            production_name: str,
            campaign_name: str,
            **kwargs):
        c_id = self.get_id(production_name, campaign_name)
        self._update_campaign(c_id, **kwargs)

    def update_step(
            self,
            production_name: str,
            campaign_name: str,
            step_name: str,
            **kwargs):
        s_id = self.get_id(production_name, campaign_name, step_name)
        self._update_step(s_id, **kwargs)

    def update_group(
            self,
            production_name: str,
            campaign_name: str,
            step_name: str,
            group_name: str,
            **kwargs):
        g_id = self.get_id(production_name, campaign_name, step_name, group_name)
        self._update_group(g_id, **kwargs)

    def update_workflow(
            self,
            production_name: str,
            campaign_name: str,
            step_name: str,
            group_name: str,
            workflow_idx: int,
            **kwargs):
        w_id = self.get_id(production_name, campaign_name, step_name, group_name, workflow_idx)
        self._update_workflow(w_id, **kwargs)

    def get_production_id(self, production_name: str) -> int:
        raise NotImplementedError()

    def get_campaign_id(self, production_id: int, campaign_name: str) -> int:
        raise NotImplementedError()

    def get_step_id(self, campaign_id: int, step_name: str) -> int:
        raise NotImplementedError()

    def get_group_id(self, step_id: int, group_name: str) -> int:
        raise NotImplementedError()

    def get_workflow_id(self, group_id: int, workflow_idx: int) -> int:
        raise NotImplementedError()

    @staticmethod
    def get_lower_level_and_args(**kwargs):
        ret_dict = dict(production_name=kwargs.get('production_name'))
        if kwargs.get('campaign_name') is None:
            return LevelEnum.production, ret_dict
        ret_dict.update(campaign_name=kwargs.get('campaign_name'))
        if kwargs.get('step_name') is None:
            return LevelEnum.campaign, ret_dict
        ret_dict.update(step_name=kwargs.get('step_name'))
        if kwargs.get('group_name') is None:
            return LevelEnum.step, ret_dict
        ret_dict.update(group_name=kwargs.get('group_name'))
        if kwargs.get('workflow_idx') is None:
            return LevelEnum.group, ret_dict
        ret_dict.update(workflow_idx=kwargs.get('workflow_idx'))
        return LevelEnum.workflow, ret_dict

    @staticmethod
    def get_upper_level_and_args(**kwargs):
        ret_dict = {}
        if kwargs.get('production_name') is None:
            return LevelEnum.production, ret_dict
        ret_dict.update(production_name=kwargs.get('production_name'))
        if kwargs.get('campaign_name') is None:
            return LevelEnum.campaign, ret_dict
        ret_dict.update(campaign_name=kwargs.get('campaign_name'))
        if kwargs.get('step_name') is None:
            return LevelEnum.step, ret_dict
        ret_dict.update(step_name=kwargs.get('step_name'))
        if kwargs.get('group_name') is None:
            return LevelEnum.group, ret_dict
        ret_dict.update(group_name=kwargs.get('group_name'))
        if kwargs.get('workflow_idx') is None:
            return LevelEnum.workflow, ret_dict
        ret_dict.update(workflow_idx=kwargs.get('workflow_idx'))
        return LevelEnum.workflow, ret_dict

    def get_id(
            self,
            production_name: str,
            campaign_name=None,
            step_name=None,
            group_name=None,
            workflow_idx=None) -> int:
        p_id = self.get_production_id(production_name)
        if campaign_name is None:
            return p_id
        c_id = self.get_campaign_id(p_id, campaign_name)
        if step_name is None:
            return c_id
        s_id = self.get_step_id(c_id, step_name)
        if group_name is None:
            return s_id
        g_id = self.get_group_id(s_id, group_name)
        if workflow_idx is None:
            return g_id
        return self.get_workflow_id(g_id, workflow_idx)

    def get_production_data(self, production_id: int):
        raise NotImplementedError()

    def get_campaign_data(self, campaign_id: int):
        raise NotImplementedError()

    def get_step_data(self, step_id: int):
        raise NotImplementedError()

    def get_group_data(self, group_id: int):
        raise NotImplementedError()

    def get_workflow_data(self, workflow_id: int):
        raise NotImplementedError()

    def get_production_iterable(self) -> Iterable:
        raise NotImplementedError()

    def get_campaign_iterable(self, production_id: int) -> Iterable:
        raise NotImplementedError()

    def get_step_iterable(self, campaign_id: int) -> Iterable:
        raise NotImplementedError()

    def get_group_iterable(self, step_id: int) -> Iterable:
        raise NotImplementedError()

    def get_workflow_iterable(self, group_id: int) -> Iterable:
        raise NotImplementedError()

    def count_productions(self) -> int:
        raise NotImplementedError()

    def count_campaigns(self, production_id: int) -> int:
        raise NotImplementedError()

    def count_steps(self, campaign_id: int) -> int:
        raise NotImplementedError()

    def count_groups(self, step_id: int) -> int:
        raise NotImplementedError()

    def count_workflows(self, group_id: int) -> int:
        raise NotImplementedError()

    def print_productions(self):
        raise NotImplementedError()

    def print_campaigns(self, production_id: int):
        raise NotImplementedError()

    def print_steps(self, campaign_id: int):
        raise NotImplementedError()

    def print_groups(self, step_id: int):
        raise NotImplementedError()

    def print_workflows(self, group_id: int):
        raise NotImplementedError()

    def _insert_production(self, **kwargs):
        raise NotImplementedError()

    def _insert_campaign(self, **kwargs):
        raise NotImplementedError()

    def _insert_step(self, **kwargs):
        raise NotImplementedError()

    def _insert_group(self, **kwargs):
        raise NotImplementedError()

    def _insert_workflow(self, **kwargs):
        raise NotImplementedError()

    def _update_production(self, production_id: int, **kwargs):
        raise NotImplementedError()

    def _update_campaign(self, campaign_id: int, **kwargs):
        raise NotImplementedError()

    def _update_step(self, step_id: int, **kwargs):
        raise NotImplementedError()

    def _update_group(self, group_id: int, **kwargs):
        raise NotImplementedError()

    def _update_workflow(self, workflow_id: int, **kwargs):
        raise NotImplementedError()
