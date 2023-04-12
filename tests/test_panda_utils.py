import os

import pytest

# from lsst.cm.tools.core import panda_utils

no_panda = "PANDA_CONFIG_ROOT" not in os.environ
pytestmark = pytest.mark.skipif(no_panda, reason="No Panda")


def test_parse_bps_stdout() -> None:
    # placeholder test until we get some example files to test this on
    pass


def test_determine_error_handling() -> None:
    # placeholder test until we get some example files to test this on
    pass


def test_decide_panda_status() -> None:
    # placeholder test until we get some example files to test this on
    pass
