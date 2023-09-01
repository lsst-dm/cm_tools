import os
import shutil
import sys

import yaml

from lsst.cm.tools.core import panda_utils
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface


def test_load_error_type() -> None:
    """"""
    # Connect to test db saved in examples.
    try:
        os.unlink("test_error_loading.db")
    except OSError:  # pragma: no cover
        pass
    # Make an interface for the test database with errors loaded already
    db_errors_already_loaded = SQLAlchemyInterface(
        "sqlite:///examples/test_error_loading.db", echo=False, create=False
    )
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir
    # Connect to new test db with no errors yet.
    try:
        os.unlink("test_error_handling.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")
    # Make an interface for the test database
    db_just_loaded_errors = SQLAlchemyInterface("sqlite:///test_error_handling.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir
    # Load error types into test database:
    db_just_loaded_errors.load_error_types("examples/configs/error_code_decisions.yaml")
    # Assert that db 1 and 2 are the same.
    error1 = db_errors_already_loaded.match_error_type("trans, 137", "Transform received signal SIGKILL")
    error2 = db_just_loaded_errors.match_error_type("trans, 137", "Transform received signal SIGKILL")
    assert error1 is not None, "Saved error database is missing test error"
    assert error2 is not None, "Database created from test file is missing test error"
    comp = error1.panda_err_code == error2.panda_err_code
    assert comp is True, "Load does not match existing database"


def test_error_handling() -> None:
    try:
        os.unlink("test_error_handling.db")
    except OSError:  # pragma: no cover
        pass
    os.system("\\rm -rf archive_test")

    iface = SQLAlchemyInterface("sqlite:///test_error_handling.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    iface.load_error_types("examples/configs/error_code_decisions.yaml")

    assert iface.match_error_type("taskbuffer, 102", "expired in pending. status unchanged") is not None

    iface.modify_error_type("expired_in_pending", diagnostic_message="expired in pending. status peachy")

    assert iface.match_error_type("taskbuffer, 102", "expired in pending. status unchanged") is None

    assert iface.match_error_type("taskbuffer, 102", "expired in pending. status peachy") is not None


def test_error_matching() -> None:
    try:
        os.unlink("test_error.db")
    except OSError:  # pragma: no cover
        pass
    shutil.rmtree("archive_requeue", ignore_errors=True)

    iface = SQLAlchemyInterface("sqlite:///test_error.db", echo=False, create=True)
    Handler.plugin_dir = "examples/handlers/"
    Handler.config_dir = "examples/configs/"
    os.environ["CM_CONFIGS"] = Handler.config_dir

    config_name = "test_errors"
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
        prod_base_url="archive_errors",
    )

    db_c_id = iface.get_db_id(production_name="example", campaign_name="test")

    step_name = "step1"
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

    with open("examples/errors.yaml", "r") as error_file:
        errors_aggregate = yaml.safe_load(error_file)

    iface.commit_errors(5, errors_aggregate)
    panda_utils.print_errors_aggregate(sys.stdout, errors_aggregate)

    status_list = ["done", "done", "finished", "finished"]
    max_pct_failed = {152029: 0.001, 152185: 0.001}
    status = panda_utils.decide_panda_status(iface, status_list, errors_aggregate, max_pct_failed)
    assert status

    iface.load_error_types("examples/configs/error_code_decisions.yaml")
    iface.rematch_errors()
    iface.report_errors(sys.stdout, LevelEnum.step, db_s_id)
    iface.report_error_trend(sys.stdout, "kron_kron")


if __name__ == "__main__":
    test_error_matching()
