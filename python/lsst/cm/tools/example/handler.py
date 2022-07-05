"""Base class to make handlers for a particular Production"""

import os

from typing import Any
from collections import OrderedDict

import yaml

from lsst.cm.tools.core.utils import StatusEnum
from lsst.cm.tools.core.grouper import Grouper
from lsst.cm.tools.core.db_interface import DbInterface, DbId
from lsst.cm.tools.db.sqlalch_handler import SQLAlchemyHandler


class ExampleStep1Grouper(Grouper):

    def _do_call(self):
        out_dict = dict(
            production_name=self.config['production_name'],
            campaign_name=self.config['campaign_name'],
            step_name=self.config['step_name'])

        for i in range(10):
            out_dict.update(group_name=f'group_{i}',
                            g_data_query_tmpl=f"i == {i}")
            yield out_dict


class ExampleStep2Grouper(Grouper):

    def _do_call(self):
        out_dict = dict(
            production_name=self.config['production_name'],
            campaign_name=self.config['campaign_name'],
            step_name=self.config['step_name'])

        for i in range(20):
            out_dict.update(group_name=f'group_{i}',
                            g_data_query_tmpl=f"i == {i}")
            yield out_dict


class ExampleStep3Grouper(Grouper):

    def _do_call(self):
        out_dict = dict(
            production_name=self.config['production_name'],
            campaign_name=self.config['campaign_name'],
            step_name=self.config['step_name'])

        for i in range(20):
            out_dict.update(group_name=f'group_{i}',
                            g_data_query_tmpl=f"i == {i}")
            yield out_dict


class ExampleHandler(SQLAlchemyHandler):

    step_dict = OrderedDict([
        ('step1', ExampleStep1Grouper),
        ('step2', ExampleStep2Grouper),
        ('step3', ExampleStep3Grouper),
    ])

    def check_workflow_status(
            self,
            dbi: DbInterface,
            db_id: DbId,
            data) -> dict[str, Any]:
        panda_log_url = data['panda_log_url']
        if not os.path.exists(panda_log_url):
            return dict(status=data['w_status'])
        with open(panda_log_url, 'rt', encoding='utf-8') as fin:
            update_fields = yaml.safe_load(fin)
        update_fields['status'] = StatusEnum[update_fields['status']]
        return update_fields

    def fake_run(
            self,
            dbi: DbInterface,
            db_id: DbId,
            data,
            status: StatusEnum = StatusEnum.completed) -> None:
        panda_log_url = data['panda_log_url']
        with open(panda_log_url, 'wt', encoding='utf-8') as fout:
            fout.write(f'status: {status.name}\n')
