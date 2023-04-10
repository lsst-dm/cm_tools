import os

import pytest

# from lsst.cm.tools.core import panda_utils

no_panda = "PANDA_CONFIG_ROOT" not in os.environ
pytestmark = pytest.mark.skipif(no_panda, reason="No Panda")


def parse_bps_stdout() -> None:
    assert False


def test_determine_error_handling() -> None:
    assert False


def test_decide_panda_status() -> None:
    assert False
