from typing import Any, Iterable

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.step_handler import StepHandler


class ExampleExtraStepHandler(StepHandler):
    """Example extra step handler"""

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )

        for i in range(10):
            out_dict.update(group_name=f"group_{i}", data_query=f"i == {i}")
            yield out_dict
