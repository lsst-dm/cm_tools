import os
from typing import Any, Iterable, Optional

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.entry_handler import GenericEntryHandler
from lsst.cm.tools.db.group import Group
from lsst.cm.tools.db.step import Step


class StepHandler(GenericEntryHandler):
    """Campaign level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. provide the parameters for the Group callback handler with the
    `group_iterator` function.
    """

    config_block = "step"

    fullname_template = os.path.join("{production_name}", "{campaign_name}", "{step_name}")

    group_handler_class: Optional[str]

    level = LevelEnum.step

    def insert(self, dbi: DbInterface, parent: Campaign, **kwargs: Any) -> Step:
        step_name = self.get_kwarg_value("step_name", **kwargs)
        insert_fields = dict(
            name=step_name,
            fullname=self.get_fullname(**kwargs),
            p_id=parent.p_.id,
            c_id=parent.id,
            config_id=parent.config_id,
            frag_id=self._fragment_id,
            data_query=kwargs.get("data_query"),
            coll_in=parent.coll_in,
            coll_source=parent.coll_in,
            status=StatusEnum.waiting,
        )
        extra_fields = dict(
            prod_base_url=parent.prod_base_url,
            root_coll=parent.root_coll,
            production_name=parent.p_.name,
            campaign_name=parent.name,
            step_name=step_name,
        )
        coll_names = self.coll_names(insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return Step.insert_values(dbi, **insert_fields)

    def make_children(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        self.make_groups(dbi, entry)
        return StatusEnum.populating

    def make_groups(self, dbi: DbInterface, entry: Step) -> dict[str, Group]:
        """Called to set up the groups needed to process this step

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        entry : Step
            The entry we are preparing

        Returns
        -------
        groups : dict[str, Group]
            The newly made Groups
        """
        out_dict = {}
        group_handler = entry.get_sub_handler("group")
        insert_fields = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
            coll_source=entry.coll_in,
        )
        for group_kwargs in self.group_iterator(dbi, entry, **insert_fields):
            insert_fields.update(**group_kwargs)
            out_dict[group_kwargs["group_name"]] = group_handler.insert(dbi, entry, **insert_fields)
        return out_dict

    def group_iterator(self, dbi: DbInterface, entry: Step, **kwargs: Any) -> Iterable:
        """Iterator of over the parameters of the Groups for this step

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        entry : Step
            The entry we are preparing

        Keywords
        --------
        These can

        Returns
        -------
        group_configs : Iterable[dict[str, Any]]
            Iterator over the configs
        """
        raise NotImplementedError()
