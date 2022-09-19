import os
import sys

import pytest

# from lsst.cm.tools.core.db_interface import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db.dependency import Dependency
from lsst.cm.tools.db.script import Script
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface


def run_production(
    iface: SQLAlchemyInterface, campaign_name: str, config_name: str, config_yaml: str
) -> None:

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")

    config = iface.parse_config(config_name, config_yaml)
    assert config

    campaign = iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name=campaign_name,
        butler_repo="repo",
        prod_base_url="archive_test",
    )
    assert campaign

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name=campaign_name)

    for step_name in ["step1", "step2", "step3"]:

        result = iface.queue_jobs(LevelEnum.campaign, db_c_id)
        # assert result

        result = iface.launch_jobs(LevelEnum.campaign, db_c_id, 5)

        # assert result
        result = iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        # assert result
        # These should fail
        result = iface.queue_jobs(LevelEnum.campaign, db_c_id)
        # assert not result
        result = iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        # assert not result
        result = iface.launch_jobs(LevelEnum.campaign, db_c_id, 0)
        # assert not result

        result = iface.accept(LevelEnum.campaign, db_c_id)
        assert not result

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
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    config_name = "test_full"
    config_yaml = "example_config.yaml"
    run_production(iface, "test1", config_name, config_yaml)

    config_name2 = "test2_full"
    config_yaml2 = "example_config2.yaml"
    run_production(iface, "test2", config_name2, config_yaml2)

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test1")
    db_s_id = iface.get_db_id(
        LevelEnum.step, production_name="example", campaign_name="test1", step_name="step1"
    )
    db_g_id = iface.get_db_id(
        LevelEnum.group,
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
    )
    db_w_id = iface.get_db_id(
        LevelEnum.workflow,
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
        workflow_idx=0,
    )

    iface.daemon(db_c_id, sleep_time=1, n_iter=3)

    check_top_id = iface.get_db_id(None)
    assert check_top_id.to_tuple() == (None, None, None, None, None)

    check_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    assert check_p_id.to_tuple() == (1, None, None, None, None)

    prod = iface.get_entry(LevelEnum.production, check_p_id)
    assert prod.db_id.to_tuple() == (1, None, None, None, None)
    assert prod.name == "example"
    assert (
        iface.get_entry_from_fullname(
            LevelEnum.production,
            "example",
        ).db_id.to_tuple()
        == prod.db_id.to_tuple()
    )

    check_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test1")
    assert check_c_id.to_tuple() == (1, 1, None, None, None)
    assert (
        iface.get_entry_from_fullname(
            LevelEnum.campaign,
            "example/test1",
        ).db_id.to_tuple()
        == check_c_id.to_tuple()
    )

    assert (
        iface.get_db_id(
            LevelEnum.campaign,
            fullname="example/test1",
        ).to_tuple()
        == check_c_id.to_tuple()
    )

    check_c_bad_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="bad")
    assert check_c_bad_id.to_tuple() == (1, None, None, None, None)

    check_c_none_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name=None)
    assert check_c_none_id.to_tuple() == (1, None, None, None, None)

    check_s_id = iface.get_db_id(
        LevelEnum.step, production_name="example", campaign_name="test1", step_name="step1"
    )
    assert check_s_id.to_tuple() == (1, 1, 1, None, None)

    check_g_id = iface.get_db_id(
        LevelEnum.group,
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
    )
    assert check_g_id.to_tuple() == (1, 1, 1, 1, None)

    result = iface.rollback(LevelEnum.campaign, db_c_id, StatusEnum.accepted)
    assert not result

    iface.rollback(LevelEnum.campaign, db_c_id, StatusEnum.waiting)
    iface.supersede(LevelEnum.campaign, db_c_id)

    with open(os.devnull, "wt") as fout:
        iface.print_config(fout, config_name)
        iface.print_table(fout, TableEnum.production)
        iface.print_table(fout, TableEnum.campaign)
        iface.print_table(fout, TableEnum.step)
        iface.print_table(fout, TableEnum.group)
        iface.print_table(fout, TableEnum.workflow)
        iface.print_table(fout, TableEnum.script)
        iface.print_table(fout, TableEnum.job)
        iface.print_table(fout, TableEnum.dependency)
        iface.print_tree(fout, LevelEnum.campaign, db_c_id)
        iface.print_tree(fout, LevelEnum.step, db_s_id)
        iface.print_tree(fout, LevelEnum.group, db_g_id)
        iface.print_tree(fout, LevelEnum.workflow, db_w_id)
        iface.print_(fout, LevelEnum.production, None)
        iface.print_(fout, LevelEnum.campaign, db_c_id)
        iface.print_(fout, LevelEnum.step, db_c_id)
        iface.print_(fout, LevelEnum.group, db_c_id)

    os.system("\\rm -rf archive_test")
    os.unlink("test.db")


def test_failed_workflows() -> None:

    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"

    config_name = "test_failed"
    config_yaml = "example_config.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    config = iface.parse_config(config_name, config_yaml)

    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        prod_base_url="archive_test",
    )

    with pytest.raises(KeyError):
        iface.insert(
            db_p_id,
            "campaign",
            config,
            production_name="example",
            campaign_name="fail_1",
            prod_base_url="archive_test",
        )
    with pytest.raises(KeyError):
        iface.insert(
            db_p_id,
            "campaign",
            config,
            production_name="example",
            campaign_name="fail_2",
            butler_repo="repo",
        )
    with pytest.raises(KeyError):
        iface.insert(
            db_p_id,
            "missing",
            config,
            production_name="example",
            campaign_name="fail_2",
            butler_repo="repo",
        )
    with pytest.raises(KeyError):
        iface.print_config(sys.stdout, "no_config")

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test")

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(
            LevelEnum.step, production_name="example", campaign_name="test", step_name=step_name
        )
        iface.queue_jobs(LevelEnum.campaign, db_c_id)
        iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
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
        db_w_id = iface.get_db_id(
            LevelEnum.workflow,
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
            workflow_idx=0,
        )
        iface.rollback(LevelEnum.workflow, db_w_id, StatusEnum.ready)
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


def test_failed_scripts() -> None:

    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"

    config_name = "test_failed"
    config_yaml = "example_failed_script.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")

    config = iface.parse_config(config_name, config_yaml)
    assert config

    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        prod_base_url="archive_test",
    )

    for step_name in ["step1"]:
        db_g_id = iface.get_db_id(
            LevelEnum.group,
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
        )
        iface.rollback(LevelEnum.group, db_g_id, StatusEnum.ready)

    iface.print_table(sys.stdout, TableEnum.production)
    iface.print_table(sys.stdout, TableEnum.campaign)
    iface.print_table(sys.stdout, TableEnum.step)
    iface.print_table(sys.stdout, TableEnum.group)
    iface.print_table(sys.stdout, TableEnum.workflow)
    iface.print_table(sys.stdout, TableEnum.script)
    iface.print_table(sys.stdout, TableEnum.job)
    iface.print_table(sys.stdout, TableEnum.dependency)

    os.system("\\rm -rf archive_test")
    os.unlink("fail.db")


def test_script_interface() -> None:

    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"

    config_name = "test_scripts"
    config_yaml = "example_test_scripts.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    config = iface.parse_config(config_name, config_yaml)
    assert config
    check_config = iface.get_config(config_name)
    assert check_config == config

    db_p_id = iface.get_db_id(LevelEnum.production, production_name="example")
    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        prod_base_url="archive_test",
    )

    db_c_id = iface.get_db_id(LevelEnum.campaign, production_name="example", campaign_name="test")

    iface.fake_script(LevelEnum.campaign, db_c_id, "prepare", StatusEnum.running)
    iface.fake_script(LevelEnum.campaign, db_c_id, "ancil", StatusEnum.running)
    iface.fake_script(LevelEnum.campaign, db_c_id, "prepare", StatusEnum.completed)
    iface.fake_script(LevelEnum.campaign, db_c_id, "ancil", StatusEnum.completed)

    result = iface.fake_script(LevelEnum.campaign, db_c_id, "ancil", StatusEnum.completed)
    assert not result

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(
            LevelEnum.step, production_name="example", campaign_name="test", step_name=step_name
        )
        iface.queue_jobs(LevelEnum.campaign, db_c_id)
        iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        iface.fake_run(LevelEnum.step, db_s_id)
        iface.fake_script(LevelEnum.step, db_s_id, "collect", StatusEnum.completed)
        iface.fake_script(LevelEnum.step, db_s_id, "validate", StatusEnum.completed)

    iface.print_table(sys.stdout, TableEnum.production)
    iface.print_table(sys.stdout, TableEnum.campaign)
    iface.print_table(sys.stdout, TableEnum.step)
    iface.print_table(sys.stdout, TableEnum.group)
    iface.print_table(sys.stdout, TableEnum.workflow)
    iface.print_table(sys.stdout, TableEnum.script)
    iface.print_table(sys.stdout, TableEnum.job)
    iface.print_table(sys.stdout, TableEnum.dependency)

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
    test_full_example()
