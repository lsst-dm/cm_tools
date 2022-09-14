import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.entry_handler import EntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.step import Step
from lsst.cm.tools.db.workflow_handler import WorkflowHandler


class GroupHandler(EntryHandler):
    """Group level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement the `xxx_hook` functions.
    2. define the Workflow callback hander with `make_workflow_handler`
    """

    config_block = "group"

    fullname_template = os.path.join(
        "{production_name}",
        "{campaign_name}",
        "{step_name}",
        "{group_name}",
    )

    level = LevelEnum.group

    def insert(self, dbi: DbInterface, parent: Step, **kwargs: Any) -> Group:
        insert_fields = dict(
            name=self.get_kwarg_value("group_name", **kwargs),
            fullname=self.get_fullname(**kwargs),
            p_id=parent.p_.id,
            c_id=parent.c_.id,
            s_id=parent.id,
            data_query=kwargs.get("data_query"),
            coll_source=parent.coll_in,
            status=StatusEnum.waiting,
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        extra_fields = dict(
            prod_base_url=parent.prod_base_url,
            root_coll=parent.root_coll,
        )
        coll_names = self.coll_names(insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return Group.insert_values(dbi, **insert_fields)

    def make_children(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        workflow_handler = self.make_workflow_handler()
        workflow_handler.insert(
            dbi,
            entry,
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.s_.name,
            group_name=entry.name,
        )
        return StatusEnum.populating

    def make_workflow_handler(self) -> WorkflowHandler:
        """Return a WorkflowHandler to manage the
        Workflows associated with the Groups managed by this
        handler
        """
        raise NotImplementedError()
