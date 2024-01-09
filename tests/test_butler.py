import os
import sys

import pytest
from lsst.daf.butler import Butler

from lsst.cm.tools.core import butler_utils

butler_main = "/sdf/group/rubin/repo/main"
butler_input_coll = "HSC/raw/RC2_subset"
butler_run_coll = "u/echarles/cm/HSC_rc2_subset/w_2023_08_test1/step1/group0/w00_000"
butler_bad_coll = "this_does_not_exist"
no_butler = not os.path.exists(butler_main)

pytestmark = pytest.mark.skipif(no_butler, reason="No Butler")


def test_print_dataset_summary() -> None:
    butler_utils.print_dataset_summary(sys.stdout, butler_main, [butler_run_coll])


def test_build_queries() -> None:
    butler = Butler(butler_main, collections=[butler_input_coll], without_datastore=True)
    queries = butler_utils.build_data_queries(butler, "raw", "exposure", min_queries=3)
    assert len(queries) >= 3


def test_clean_collection_set() -> None:
    butler = Butler(butler_main, collections=[butler_input_coll], without_datastore=True)
    clean_colls = butler_utils.clean_collection_set(butler, [[butler_input_coll, butler_bad_coll]])
    assert len(clean_colls) == 1
