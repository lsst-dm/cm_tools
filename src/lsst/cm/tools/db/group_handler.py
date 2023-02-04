import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.entry_handler import GenericEntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.step import Step


class GroupHandler(GenericEntryHandler):
    """Group level callback handler

    Provides interface functions.
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
        group_name = self.get_kwarg_value("group_name", **kwargs)
        insert_fields = dict(
            name=group_name,
            fullname=self.get_fullname(**kwargs),
            p_id=parent.p_.id,
            c_id=parent.c_.id,
            s_id=parent.id,
            config_id=parent.config_id,
            frag_id=self._fragment_id,
            data_query=kwargs.get("data_query"),
            coll_source=parent.coll_in,
            status=StatusEnum.waiting,
        )
        extra_fields = dict(
            prod_base_url=parent.prod_base_url,
            root_coll=parent.root_coll,
            production_name=parent.p_.name,
            campaign_name=parent.c_.name,
            step_name=parent.name,
            group_name=group_name,
        )
        coll_names = self.coll_names(insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return Group.insert_values(dbi, **insert_fields)

    def make_children(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        if self.config.get("scripts", {}).get("prepare") is None:
            data_query = entry.data_query
        workflow_handler = entry.get_sub_handler("workflow")
        workflow_handler.insert(
            dbi,
            entry,
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.s_.name,
            group_name=entry.name,
            data_query=data_query,
        )
        return StatusEnum.populating
