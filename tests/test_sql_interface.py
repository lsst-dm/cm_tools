import os
import sys

from lsst.cm.tools.core.utils import LevelEnum
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.db_interface import DbId
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface


def test_sql_interface():

    try:
        os.unlink('test.db')
    except OSError:
        pass

    iface = SQLAlchemyInterface('sqlite:///test.db', echo=False, create=True)

    config_yaml = "examples/example_config.yaml"
    handler_class = "lsst.cm.tools.example.handler.ExampleHandler"
    the_handler = Handler.get_handler(handler_class, config_yaml)

    top_db_id = DbId()
    iface.insert(LevelEnum.production, top_db_id, the_handler, production_name='example')

    db_p_id = iface.get_db_id(LevelEnum.production, production_name='example')
    iface.insert(
        LevelEnum.campaign,
        db_p_id,
        the_handler,
        recurse=True,
        production_name='example',
        campaign_name='test')

    for step_name in ['step1', 'step2', 'step3']:
        db_s_id = iface.get_db_id(
            LevelEnum.step,
            production_name='example',
            campaign_name='test',
            step_name=step_name)
        iface.prepare(LevelEnum.step, db_s_id, recurse=True)
        iface.queue_workflows(LevelEnum.step, db_s_id)
        iface.launch_workflows(LevelEnum.step, db_s_id, 100)
        iface.fake_run(db_s_id)
        iface.check(LevelEnum.workflow, db_s_id, recurse=True)
        iface.accept(LevelEnum.workflow, db_s_id)
        iface.check(LevelEnum.group, db_s_id, recurse=True)
        iface.accept(LevelEnum.group, db_s_id)
        iface.check(LevelEnum.step, db_s_id, recurse=True)
        iface.accept(LevelEnum.step, db_s_id)

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name='example', campaign_name='test')
    iface.check(LevelEnum.campaign, db_c_id, recurse=True)
    iface.accept(LevelEnum.campaign, db_c_id)

    iface.print_table(sys.stdout, LevelEnum.production)
    iface.print_table(sys.stdout, LevelEnum.campaign)
    iface.print_table(sys.stdout, LevelEnum.step)
    iface.print_table(sys.stdout, LevelEnum.group)
    iface.print_table(sys.stdout, LevelEnum.workflow)
