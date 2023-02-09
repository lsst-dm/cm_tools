import os
from typing import Any, Iterable

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker, write_status_to_yaml
from lsst.cm.tools.core.utils import StatusEnum
from lsst.cm.tools.db.job_handler import JobHandler
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.step_handler import StepHandler

if os.environ.get("CM_PROFILE", 0) == "1":  # pragma: no cover
    NGROUP1 = 500
    NGROUP2 = 500
    NGROUP3 = 500
else:
    NGROUP1 = 10
    NGROUP2 = 20
    NGROUP3 = 20


class ExampleJobHandler(JobHandler):
    """Example job callback handler"""

    yaml_checker_class = YamlChecker().get_checker_class_name()
    fake_rollback_class = FakeRollback().get_rollback_class_name()

    def fake_run_hook(
        self, dbi: DbInterface, job: JobBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        write_status_to_yaml(job.stamp_url, status)


class ExampleStep1Handler(StepHandler):
    """Example step handler (1 of 3)"""

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(NGROUP1):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep2Handler(StepHandler):
    """Example step handler (2 of 3)"""

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(NGROUP2):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict


class ExampleStep3Handler(StepHandler):
    """Example step handler (3 of 3)"""

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(NGROUP3):
            out_dict.update(group_name=f"group{i}", data_query=f"i == {i}")
            yield out_dict
