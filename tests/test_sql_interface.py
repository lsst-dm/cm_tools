import os

import pytest

# from lsst.cm.tools.core.db_interface import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db.dependency import Dependency
from lsst.cm.tools.db.script import Script
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface


def run_production(iface: SQLAlchemyInterface, the_handler: Handler, campaign_name) -> None:

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    iface.insert(
        db_p_id,
        the_handler,
        production_name="example",
        campaign_name=campaign_name,
        butler_repo="repo",
        prod_base_url="archive_test",
    )

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name=campaign_name)
    result = iface.prepare(LevelEnum.campaign, db_c_id)
    assert result

    result = iface.prepare(LevelEnum.campaign, db_c_id)
    assert not result

    db_s3_id = iface.get_db_id(
        LevelEnum.step, production_name="example", campaign_name=campaign_name, step_name="step3"
    )

    # This should fail
    result = iface.prepare(LevelEnum.step, db_s3_id)
    assert not result

    for step_name in ["step1", "step2", "step3"]:
        db_s_id = iface.get_db_id(
            LevelEnum.step, production_name="example", campaign_name=campaign_name, step_name=step_name
        )

        # This should fail (already prepared from above)
        result = iface.prepare(LevelEnum.campaign, db_c_id)
        assert not result

        # This should fail
        result = iface.prepare(LevelEnum.campaign, db_s_id)
        assert not result

        result = iface.queue_workflows(LevelEnum.campaign, db_c_id)
        # assert result

        result = iface.launch_workflows(LevelEnum.campaign, db_c_id, 5)
        # assert result
        result = iface.launch_workflows(LevelEnum.campaign, db_c_id, 100)
        # assert result
        # These should fail
        result = iface.queue_workflows(LevelEnum.campaign, db_c_id)
        # assert not result
        result = iface.launch_workflows(LevelEnum.campaign, db_c_id, 100)
        # assert not result
        result = iface.launch_workflows(LevelEnum.campaign, db_c_id, 0)
        # assert not result

        result = iface.fake_run(LevelEnum.campaign, db_c_id)
        # assert result

        result = iface.accept(LevelEnum.campaign, db_c_id)
        # assert result
        result = iface.fake_run(LevelEnum.campaign, db_c_id)
        # assert not result


def test_full_example() -> None:

    try:
        os.unlink("test.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///test.db", echo=False, create=True)

    config_yaml = "examples/example_config.yaml"
    handler1_class = "lsst.cm.tools.example.handler.ExampleHandler"
    handler1 = Handler.get_handler(handler1_class, config_yaml)

    handler2_class = "lsst.cm.tools.example.handler2.Example2Handler"
    handler2 = Handler.get_handler(handler2_class, config_yaml)

    top_db_id = None
    iface.insert(top_db_id, None, production_name="example")

    run_production(iface, handler1, "test1")
    run_production(iface, handler2, "test2")

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test1")
    db_s_id = iface.get_db_id(
        LevelEnum.step, production_name="example", campaign_name="test1", step_name="step1"
    )

    iface.daemon(db_c_id, sleep_time=1, n_iter=3)
    with open(os.devnull, "wt") as fout:
        iface.print_table(fout, TableEnum.production)
        iface.print_table(fout, TableEnum.campaign)
        iface.print_table(fout, TableEnum.step)
        iface.print_table(fout, TableEnum.group)
        iface.print_table(fout, TableEnum.workflow)
        iface.print_tree(fout, LevelEnum.campaign, db_c_id)
        iface.print_tree(fout, LevelEnum.step, db_s_id)
        iface.print_(fout, LevelEnum.production, None)
        iface.print_(fout, LevelEnum.campaign, db_c_id)
        iface.print_(fout, LevelEnum.step, db_c_id)
        iface.print_(fout, LevelEnum.group, db_c_id)

    check_top_id = iface.get_db_id(None)
    assert check_top_id.to_tuple() == (None, None, None, None)

    check_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    assert check_p_id.to_tuple() == (1, None, None, None)

    prod = iface.get_entry(LevelEnum.production, check_p_id)
    assert prod.db_id.to_tuple() == (1, None, None, None)
    assert prod.name == "example"

    check_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test1")
    assert check_c_id.to_tuple() == (1, 1, None, None)

    check_c_bad_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="bad")
    assert check_c_bad_id.to_tuple() == (1, None, None, None)

    check_c_none_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name=None)
    assert check_c_none_id.to_tuple() == (1, None, None, None)

    check_s_id = iface.get_db_id(
        LevelEnum.step, production_name="example", campaign_name="test1", step_name="step1"
    )
    assert check_s_id.to_tuple() == (1, 1, 1, None)

    check_g_id = iface.get_db_id(
        LevelEnum.group,
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
    )
    assert check_g_id.to_tuple() == (1, 1, 1, 1)

    result = iface.rollback(LevelEnum.campaign, db_c_id, StatusEnum.accepted)
    assert not result

    iface.rollback(LevelEnum.campaign, db_c_id, StatusEnum.waiting)

    os.system("\\rm -rf archive_test")
    os.unlink("test.db")


def test_failed_workflows() -> None:

    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)

    config_yaml = "examples/example_config.yaml"
    handler_class = "lsst.cm.tools.example.handler.ExampleHandler"
    the_handler = Handler.get_handler(handler_class, config_yaml)

    top_db_id = None
    iface.insert(top_db_id, None, production_name="example")

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    iface.insert(
        db_p_id,
        the_handler,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        prod_base_url="archive_test",
    )

    with pytest.raises(KeyError):
        iface.insert(
            db_p_id,
            the_handler,
            production_name="example",
            campaign_name="fail_1",
            prod_base_url="archive_test",
        )
    with pytest.raises(KeyError):
        iface.insert(
            db_p_id,
            the_handler,
            production_name="example",
            campaign_name="fail_2",
            butler_repo="repo",
        )

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test")
    iface.prepare(LevelEnum.campaign, db_c_id)

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(
            LevelEnum.step, production_name="example", campaign_name="test", step_name=step_name
        )
        iface.queue_workflows(LevelEnum.campaign, db_c_id)
        iface.launch_workflows(LevelEnum.campaign, db_c_id, 100)
        db_g_id = iface.get_db_id(
            LevelEnum.group,
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
        )
        iface.fake_run(LevelEnum.group, db_g_id, StatusEnum.failed)
        iface.fake_run(LevelEnum.step, db_s_id)
        iface.accept(LevelEnum.step, db_s_id)
        iface.reject(LevelEnum.group, db_g_id)
        db_g_id_ok = iface.get_db_id(
            LevelEnum.group,
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_5",
        )
        with pytest.raises(ValueError):
            iface.reject(LevelEnum.group, db_g_id_ok)
        iface.reject(LevelEnum.step, db_s_id)
    iface.reject(LevelEnum.campaign, db_c_id)

    iface2 = SQLAlchemyInterface("sqlite:///fail.db", echo=False)
    assert iface2
    os.system("\\rm -rf archive_test")
    os.unlink("fail.db")


def test_bad_db() -> None:

    with pytest.raises(RuntimeError):
        SQLAlchemyInterface("sqlite:///bad.db", echo=False)


def test_table_repr() -> None:

    depend = Dependency()
    assert repr(depend)

    script = Script(status=StatusEnum.ready)
    assert repr(script)


if __name__ == "__main__":
    test_failed_workflows()
