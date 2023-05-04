import os
import shutil
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
    db_p_id = iface.get_db_id(production_name="example")

    config = iface.parse_config(config_name, config_yaml)
    assert config

    campaign = iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name=campaign_name,
        lsst_version="dummy",
        butler_repo="repo",
        prod_base_url="archive_test",
    )
    assert campaign

    db_c_id = iface.get_db_id(production_name="example", campaign_name=campaign_name)

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
    shutil.rmtree("archive_test", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///test.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    config_name = "test_full"
    config_yaml = "example_config.yaml"
    run_production(iface, "test1", config_name, config_yaml)

    config_name2 = "test2_full"
    config_yaml2 = "example_config2.yaml"
    run_production(iface, "test2", config_name2, config_yaml2)

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test1")
    db_s_id = iface.get_db_id(production_name="example", campaign_name="test1", step_name="step1")
    db_g_id = iface.get_db_id(
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
    )
    db_w_id = iface.get_db_id(
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
        workflow_idx=0,
    )

    iface.daemon(db_c_id, sleep_time=1, n_iter=3)
    iface.set_script_status(LevelEnum.campaign, db_c_id, "ancil", 0, StatusEnum.reviewable)
    iface.daemon(db_c_id, sleep_time=1, n_iter=3, verbose=True, log_file="daemon_mutterings.txt")

    assert os.path.exists("daemon_mutterings.txt")
    os.unlink("daemon_mutterings.txt")

    check_top_id = iface.get_db_id()
    assert check_top_id.to_tuple() == (None, None, None, None, None)

    check_p_id = iface.get_db_id(production_name="example")
    assert check_p_id.to_tuple() == (1, None, None, None, None)

    prod = iface.get_entry(LevelEnum.production, check_p_id)
    assert prod.db_id.to_tuple() == (1, None, None, None, None)
    assert prod.name == "example"
    assert (
        iface.get_entry_from_fullname(
            "example",
        ).db_id.to_tuple()
        == prod.db_id.to_tuple()
    )

    check_c_id = iface.get_db_id(production_name="example", campaign_name="test1")
    assert check_c_id.to_tuple() == (1, 1, None, None, None)
    assert (
        iface.get_entry_from_fullname(
            "example/test1",
        ).db_id.to_tuple()
        == check_c_id.to_tuple()
    )

    assert (
        iface.get_db_id(
            fullname="example/test1",
        ).to_tuple()
        == check_c_id.to_tuple()
    )

    check_c_bad_id = iface.get_db_id(production_name="example", campaign_name="bad")
    assert check_c_bad_id.to_tuple() == (1, None, None, None, None)

    check_c_none_id = iface.get_db_id(production_name="example", campaign_name=None)
    assert check_c_none_id.to_tuple() == (1, None, None, None, None)

    check_s_id = iface.get_db_id(production_name="example", campaign_name="test1", step_name="step1")
    assert check_s_id.to_tuple() == (1, 1, 1, None, None)

    check_g_id = iface.get_db_id(
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
    )
    assert check_g_id.to_tuple() == (1, 1, 1, 1, None)

    result = iface.rollback(LevelEnum.campaign, db_c_id, StatusEnum.accepted)
    assert not result

    check_w_id = iface.get_db_id(fullname="example/test1/step1/group_0/00")
    assert check_w_id.to_tuple() == (1, 1, 1, 1, 1)

    check_w_id_2 = iface.get_db_id(
        production_name="example",
        campaign_name="test1",
        step_name="step1",
        group_name="group_0",
        workflow_idx=0,
    )
    assert check_w_id_2.to_tuple() == (1, 1, 1, 1, 1)

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

    shutil.rmtree("archive_test")
    os.unlink("test.db")


def test_failed_workflows() -> None:
    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_test", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    config_name = "test_failed"
    config_yaml = "example_config.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    db_p_id = iface.get_db_id(production_name="example")
    config = iface.parse_config(config_name, config_yaml)

    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        lsst_version="dummy",
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
            lsst_version="dummy",
            production_name="example",
            campaign_name="fail_2",
            butler_repo="repo",
        )
    with pytest.raises(KeyError):
        iface.insert(
            db_p_id,
            "missing",
            config,
            lsst_version="dummy",
            production_name="example",
            campaign_name="fail_2",
            butler_repo="repo",
        )
    with pytest.raises(KeyError):
        iface.print_config(sys.stdout, "no_config")

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test")

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(production_name="example", campaign_name="test", step_name=step_name)
        iface.queue_jobs(LevelEnum.campaign, db_c_id)
        iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        db_g_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
        )
        iface.fake_run(LevelEnum.group, db_g_id, StatusEnum.failed)
        iface.fake_run(LevelEnum.step, db_s_id)
        iface.accept(LevelEnum.step, db_s_id)
        db_w_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
            workflow_idx=0,
        )
        iface.rollback(LevelEnum.workflow, db_w_id, StatusEnum.ready)
        iface.reject(LevelEnum.group, db_g_id)
        db_g_id_ok = iface.get_db_id(
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
    shutil.rmtree("archive_test")
    os.unlink("fail.db")


def test_recover_failed() -> None:
    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_test", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    config_name = "test_failed"
    config_yaml = "example_config.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    db_p_id = iface.get_db_id(production_name="example")
    config = iface.parse_config(config_name, config_yaml)

    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        lsst_version="dummy",
        prod_base_url="archive_test",
    )

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test")

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(production_name="example", campaign_name="test", step_name=step_name)
        iface.queue_jobs(LevelEnum.campaign, db_c_id)
        iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        db_g_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
        )
        iface.fake_run(LevelEnum.group, db_g_id, StatusEnum.failed)
        iface.fake_run(LevelEnum.step, db_s_id)
        iface.accept(LevelEnum.step, db_s_id)
        db_w_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
            workflow_idx=0,
        )
        iface.supersede_job(LevelEnum.workflow, db_w_id, "job")
        result = iface.supersede_job(LevelEnum.workflow, db_w_id, "no_job")
        assert not result
        iface.add_job(db_w_id, "job")
        iface.fake_run(LevelEnum.group, db_g_id)
        iface.insert(
            db_s_id,
            "group",
            None,
            production_name="example",
            campaign_name="test",
            step_name="step1",
            group_name="extra_group",
        )

    shutil.rmtree("archive_test")
    os.unlink("fail.db")


def test_failed_scripts() -> None:
    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_test", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"

    config_name = "test_failed"
    config_yaml = "example_failed_script.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    db_p_id = iface.get_db_id(production_name="example")

    config = iface.parse_config(config_name, config_yaml)
    assert config

    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        lsst_version="dummy",
        prod_base_url="archive_test",
    )

    for step_name in ["step1"]:
        db_g_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
        )
        iface.rollback(LevelEnum.group, db_g_id, StatusEnum.ready)

    with open(os.devnull, "wt") as fout:
        iface.print_table(fout, TableEnum.production)
        iface.print_table(fout, TableEnum.campaign)
        iface.print_table(fout, TableEnum.step)
        iface.print_table(fout, TableEnum.group)
        iface.print_table(fout, TableEnum.workflow)
        iface.print_table(fout, TableEnum.script)
        iface.print_table(fout, TableEnum.job)
        iface.print_table(fout, TableEnum.dependency)

    shutil.rmtree("archive_test", ignore_errors=True)
    os.unlink("fail.db")


def test_script_interface() -> None:
    try:
        os.unlink("fail.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_test", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///fail.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    config_name = "test_scripts"
    config_yaml = "example_test_scripts.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    config = iface.parse_config(config_name, config_yaml)
    assert config
    check_config = iface.get_config(config_name)
    assert check_config == config
    mod_config = iface.parse_config("mod_config", "example_mod_config.yaml")
    assert mod_config

    db_p_id = iface.get_db_id(production_name="example")
    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        lsst_version="dummy",
        prod_base_url="archive_test",
    )

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test")

    iface.fake_script(LevelEnum.campaign, db_c_id, "prepare", StatusEnum.running)
    iface.fake_script(LevelEnum.campaign, db_c_id, "ancil", StatusEnum.running)
    iface.fake_script(LevelEnum.campaign, db_c_id, "prepare", StatusEnum.completed)
    iface.fake_script(LevelEnum.campaign, db_c_id, "ancil", StatusEnum.completed)

    result = iface.fake_script(LevelEnum.campaign, db_c_id, "ancil", StatusEnum.completed)
    assert not result

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(production_name="example", campaign_name="test", step_name=step_name)
        iface.queue_jobs(LevelEnum.campaign, db_c_id)
        iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        iface.fake_run(LevelEnum.step, db_s_id)
        iface.fake_script(LevelEnum.step, db_s_id, "collect", StatusEnum.completed)
        iface.fake_script(LevelEnum.step, db_s_id, "validate", StatusEnum.failed)
        iface.supersede_script(LevelEnum.step, db_s_id, "validate")
        iface.add_script(db_s_id, "validate")
        iface.set_script_status(LevelEnum.step, db_s_id, "validate", idx=1, status=StatusEnum.running)
        check_step = iface.get_entry(LevelEnum.step, db_s_id)
        for script_ in check_step.scripts_:
            if script_.name != "validate":
                continue
            if script_.superseded:
                continue
            assert script_.status == StatusEnum.running
        check_step.print_full()
        check_step.print_formatted(sys.stdout, "{status}")
        iface.set_script_status(LevelEnum.step, db_s_id, "validate", idx=1, status=StatusEnum.failed)
        iface.rerun_scripts(LevelEnum.step, db_s_id, "validate")

    with open(os.devnull, "wt") as fout:
        iface.print_table(fout, TableEnum.production)
        iface.print_table(fout, TableEnum.campaign)
        iface.print_table(fout, TableEnum.step)
        iface.print_table(fout, TableEnum.group)
        iface.print_table(fout, TableEnum.workflow)
        iface.print_table(fout, TableEnum.script)
        iface.print_table(fout, TableEnum.job)
        iface.print_table(fout, TableEnum.dependency)

    shutil.rmtree("archive_test")
    os.unlink("fail.db")


def test_insert() -> None:
    try:
        os.unlink("test.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_test", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///test.db", echo=False, create=True)

    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    config_name = "test"
    config_yaml = "example_config.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")
    db_p_id = iface.get_db_id(production_name="example")

    config = iface.parse_config(config_name, config_yaml)
    assert config

    campaign = iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        lsst_version="dummy",
        prod_base_url="archive_test",
    )
    assert campaign

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test")

    iface.queue_jobs(LevelEnum.campaign, db_c_id)
    iface.launch_jobs(LevelEnum.campaign, db_c_id, 5)

    db_s_id = iface.get_db_id(
        production_name="example",
        campaign_name="test",
        step_name="step1",
    )

    new_group = iface.insert(
        db_s_id,
        "group",
        config,
        production_name="example",
        campaign_name="test",
        step_name="step1",
        group_name="extra_group",
    )
    assert new_group

    new_step_config = iface.extend_config(config_name, "example_extra_step.yaml")

    new_step = iface.insert(
        db_c_id,
        "extra_step",
        new_step_config,
        production_name="example",
        campaign_name="test",
        step_name="extra_step",
        coll_source="{root_coll}/{fullname}_input",
    )
    assert new_step

    shutil.rmtree("archive_test")
    os.unlink("test.db")


def test_rescue() -> None:
    try:
        os.unlink("rescue.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_rescue", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///rescue.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    config_name = "test_failed"
    config_yaml = "example_config.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    db_p_id = iface.get_db_id(production_name="example")
    config = iface.parse_config(config_name, config_yaml)

    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        lsst_version="dummy",
        prod_base_url="archive_rescue",
    )

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test")

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(production_name="example", campaign_name="test", step_name=step_name)
        iface.queue_jobs(LevelEnum.campaign, db_c_id)
        iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        db_g_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
        )
        db_w_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
            workflow_idx=0,
        )
        iface.fake_run(LevelEnum.group, db_g_id, StatusEnum.reviewable)
        iface.fake_run(LevelEnum.step, db_s_id)
        iface.set_job_status(LevelEnum.step, db_w_id, "job", 0, StatusEnum.rescuable)
        iface.insert_rescue(db_w_id, "workflow_rescue")
        iface.print_table(sys.stdout, TableEnum.workflow)
        iface.print_table(sys.stdout, TableEnum.job)

    shutil.rmtree("archive_rescue")
    os.unlink("rescue.db")


def test_requeue() -> None:
    try:
        os.unlink("requeue.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_requeue", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///requeue.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    config_name = "test_failed"
    config_yaml = "example_config.yaml"

    top_db_id = None
    iface.insert(top_db_id, None, None, production_name="example")

    db_p_id = iface.get_db_id(production_name="example")
    config = iface.parse_config(config_name, config_yaml)

    iface.insert(
        db_p_id,
        "campaign",
        config,
        production_name="example",
        campaign_name="test",
        butler_repo="repo",
        lsst_version="dummy",
        prod_base_url="archive_requeue",
    )

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test")

    for step_name in ["step1"]:
        db_s_id = iface.get_db_id(production_name="example", campaign_name="test", step_name=step_name)
        iface.queue_jobs(LevelEnum.campaign, db_c_id)
        iface.launch_jobs(LevelEnum.campaign, db_c_id, 100)
        db_g_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
        )
        db_w_id = iface.get_db_id(
            production_name="example",
            campaign_name="test",
            step_name=step_name,
            group_name="group_4",
            workflow_idx=0,
        )
        iface.fake_run(LevelEnum.group, db_g_id, StatusEnum.reviewable)
        iface.fake_run(LevelEnum.step, db_s_id)
        iface.set_job_status(LevelEnum.step, db_w_id, "job", 0, StatusEnum.failed)
        iface.requeue_jobs(LevelEnum.workflow, db_w_id)
        iface.queue_jobs(LevelEnum.workflow, db_w_id)
        iface.launch_jobs(LevelEnum.campaign, db_w_id, 100)

        iface.print_table(sys.stdout, TableEnum.workflow)
        iface.print_table(sys.stdout, TableEnum.job)

    shutil.rmtree("archive_requeue")
    os.unlink("requeue.db")


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
