import os

import pytest

# from lsst.cm.tools.core import butler_utils

butler_main = "/sdf/group/rubin/repo/main"
no_butler = not os.path.exists(butler_main)

pytestmark = pytest.mark.skipif(no_butler, reason="No Butler")


def test_print_dataset_summary() -> None:
    assert False


def test_build_queries() -> None:
    assert False


def test_clean_collection_set() -> None:
    assert False


def test_associate_kludge() -> None:
    assert False
