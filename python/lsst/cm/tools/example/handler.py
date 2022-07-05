"""Base class to make handlers for a particular Production"""

from collections import OrderedDict

from lsst.cm.tools.core.grouper import Grouper
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
