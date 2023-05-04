import os

import pytest

from lsst.cm.tools.core import panda_utils

no_panda = "PANDA_CONFIG_ROOT" not in os.environ
pytestmark = pytest.mark.skipif(no_panda, reason="No Panda")


def test_parse_bps_stdout() -> None:
    job_pars = panda_utils.parse_bps_stdout("examples/job_000.log")
    assert job_pars["Run Id"].strip() == "3516"


def test_decide_panda_status() -> None:
    assert panda_utils.decide_panda_status(None, ["running"], {}, {}) == "running"
    assert panda_utils.decide_panda_status(None, ["failed"], {}, {}) == "failed"
    assert panda_utils.decide_panda_status(None, ["done"], {}, {}) == "done"
    assert panda_utils.decide_panda_status(None, [], {}, {}) == "running"
