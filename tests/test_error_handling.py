import os
import shutil
import sys

import yaml

from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface


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

    iface.load_error_types("examples/configs/error_code_decisions.yaml")
    iface.rematch_errors()
    iface.report_errors(sys.stdout, LevelEnum.step, db_s_id)
    iface.report_error_trend(sys.stdout, "kron_kron")


if __name__ == "__main__":
    test_error_matching()
