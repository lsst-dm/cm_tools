import os

from lsst.cm.tools.core.handler import Handler
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


if __name__ == "__main__":
    test_error_handling()
