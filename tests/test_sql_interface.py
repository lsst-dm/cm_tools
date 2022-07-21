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
import sys

import pytest
from lsst.cm.tools.core.db_interface import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface
from lsst.cm.tools.db import db


def test_full_example():

    try:
        os.unlink("test.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///test.db", echo=False, create=True)

    config_yaml = "examples/example_config.yaml"
    handler_class = "lsst.cm.tools.example.handler.ExampleHandler"
    the_handler = Handler.get_handler(handler_class, config_yaml)

    top_db_id = DbId()
    iface.insert(LevelEnum.production, top_db_id, the_handler, production_name="example")
    iface.check(LevelEnum.production, top_db_id)

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    iface.insert(
        LevelEnum.campaign,
        db_p_id,
        the_handler,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        prod_base_url="archive_test",
    )

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test")
    iface.prepare(LevelEnum.campaign, db_c_id)

    db_s3_id = iface.get_db_id(
        LevelEnum.step, production_name="example", campaign_name="test", step_name="step3"
    )

    # These should all fail
    result = iface.prepare(LevelEnum.step, db_s3_id)
    assert not result

    for step_name in ["step1", "step2", "step3"]:
        db_s_id = iface.get_db_id(
            LevelEnum.step, production_name="example", campaign_name="test", step_name=step_name
        )
        iface.prepare(LevelEnum.step, db_s_id)
        # This should fail
        result = iface.prepare(LevelEnum.step, db_s_id)
        assert not result
        expected_count = dict(step1=10, step2=30, step3=50)
        assert iface.count(LevelEnum.workflow, db_s_id) == expected_count[step_name]
        iface.queue_workflows(LevelEnum.step, db_s_id)
        iface.launch_workflows(LevelEnum.step, db_s_id, 5)
        iface.launch_workflows(LevelEnum.step, db_s_id, 100)
        # These should fail
        result = iface.queue_workflows(LevelEnum.step, db_s_id)
        assert not result
        result = iface.launch_workflows(LevelEnum.step, db_s_id, 100)
        assert not result
        # Ok, this is ok
        iface.launch_workflows(LevelEnum.step, db_s_id, 0)
        iface.fake_run(db_s_id)
        iface.accept(LevelEnum.step, db_s_id, recurse=True)
        result = iface.fake_run(db_s_id)
        assert not result

    iface.accept(LevelEnum.campaign, db_c_id)

    iface.daemon(db_c_id, sleep_time=1, n_iter=3)
    iface.print_table(sys.stdout, LevelEnum.production)
    iface.print_table(sys.stdout, LevelEnum.campaign)
    iface.print_table(sys.stdout, LevelEnum.step)
    iface.print_table(sys.stdout, LevelEnum.group)
    iface.print_table(sys.stdout, LevelEnum.workflow)

    check_top_id = iface.get_db_id(None)
    assert check_top_id.to_tuple() == (None, None, None, None, None)

    check_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    assert check_p_id.to_tuple() == (1, None, None, None, None)
    iface.print_(sys.stdout, LevelEnum.production, check_p_id)
    iface.print_(sys.stdout, LevelEnum.production, check_top_id)

    prod = iface.get_data(LevelEnum.production, check_p_id)
    assert prod.db_id.to_tuple() == (1, None, None, None, None)
    assert prod.name == 'example'

    check_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test")
    assert check_c_id.to_tuple() == (1, 1, None, None, None)

    check_c_bad_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="bad")
    assert check_c_bad_id.to_tuple() == (1, None, None, None, None)

    check_c_none_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name=None)
    assert check_c_none_id.to_tuple() == (1, None, None, None, None)

    check_s_id = iface.get_db_id(
        LevelEnum.step, production_name="example", campaign_name="test", step_name="step1"
    )
    assert check_s_id.to_tuple() == (1, 1, 1, None, None)

    check_g_id = iface.get_db_id(
        LevelEnum.group,
        production_name="example",
        campaign_name="test",
        step_name="step1",
        group_name="group_0",
    )
    assert check_g_id.to_tuple() == (1, 1, 1, 1, None)

    check_w_id = iface.get_db_id(
        LevelEnum.workflow,
        production_name="example",
        campaign_name="test",
        step_name="step1",
        group_name="group_0",
        workflow_idx=0,
    )
    assert check_w_id.to_tuple() == (1, 1, 1, 1, 1)

    os.system("\\rm -rf archive_test")
    os.unlink("test.db")


def test_failed_workflows():

    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)

    config_yaml = "examples/example_config.yaml"
    handler_class = "lsst.cm.tools.example.handler.ExampleHandler"
    the_handler = Handler.get_handler(handler_class, config_yaml)

    top_db_id = DbId()
    iface.insert(LevelEnum.production, top_db_id, the_handler, production_name="example")

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    iface.insert(
        LevelEnum.campaign,
        db_p_id,
        the_handler,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        prod_base_url="archive_test",
    )

    with pytest.raises(KeyError):
        iface.insert(
            LevelEnum.campaign,
            db_p_id,
            the_handler,
            production_name="example",
            campaign_name="fail_1",
            prod_base_url="archive_test",
        )
    with pytest.raises(KeyError):
        iface.insert(
            LevelEnum.campaign,
            db_p_id,
            the_handler,
            production_name="example",
            campaign_name="fail_2",
            butler_repo="repo",
        )

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(
            LevelEnum.step, production_name="example", campaign_name="test", step_name=step_name
        )
        iface.prepare(LevelEnum.step, db_s_id)
        iface.check(LevelEnum.workflow, db_s_id)
        iface.queue_workflows(LevelEnum.step, db_s_id)
        iface.launch_workflows(LevelEnum.step, db_s_id, 100)
        db_w_id = iface.get_db_id(
            LevelEnum.workflow,
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
            workflow_idx=0,
        )
        iface.fake_run(db_s_id)
        iface.fake_run(db_w_id, StatusEnum.failed)
        iface.accept(LevelEnum.step, db_s_id, recurse=True)
        iface.reject(LevelEnum.workflow, db_s_id)
        iface.check(LevelEnum.group, db_w_id)

        iface2 = SQLAlchemyInterface("sqlite:///fail.db", echo=False)
        assert iface2
        os.system("\\rm -rf archive_test")
        os.unlink("fail.db")


def test_bad_db():

    with pytest.raises(RuntimeError):
        SQLAlchemyInterface("sqlite:///bad.db", echo=False)


def test_table_repr():

    depend = db.Dependency()
    assert repr(depend)

    script = db.Script(status=StatusEnum.ready)
    assert repr(script)
