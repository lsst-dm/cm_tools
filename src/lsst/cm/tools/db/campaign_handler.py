import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.dependency import Dependency
from lsst.cm.tools.db.entry_handler import GenericEntryHandler
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step

# import datetime


class CampaignHandler(GenericEntryHandler):
    """Campaign level callback handler

    Provides interface functions.

    """

    config_block = "campaign"

    fullname_template = os.path.join("{production_name}", "{campaign_name}")

    level = LevelEnum.campaign

    def insert(self, dbi: DbInterface, parent: Production, **kwargs: Any) -> Campaign:
        if "butler_repo" not in kwargs:
            raise KeyError("butler_repo must be specified with inserting a campaign")
        if "prod_base_url" not in kwargs:
            raise KeyError("prod_base_url must be specified with inserting a campaign")
        if "config_id" not in kwargs:
            raise KeyError("config_id must be specified with inserting a campaign")
        insert_fields = dict(
            name=self.get_kwarg_value("campaign_name", **kwargs),
            fullname=self.get_fullname(**kwargs),
            data_query=self.get_config_var("data_query", None, **kwargs),
            root_coll=self.get_config_var("root_coll", "prod", **kwargs),
            p_id=parent.id,
            config_id=kwargs["config_id"],
            frag_id=self._fragment_id,
            status=StatusEnum.waiting,
            butler_repo=kwargs["butler_repo"],
            prod_base_url=kwargs["prod_base_url"],
        )
        coll_names = self.coll_names(insert_fields)
        insert_fields.update(**coll_names)
        campaign = Campaign.insert_values(dbi, **insert_fields)
        return campaign

    def make_children(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        self.make_steps(dbi, entry)
        return StatusEnum.populating

    def make_steps(self, dbi: DbInterface, campaign: Campaign) -> dict[str, Step]:
        """Called to set up the Steps needed to process this campaign

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        campaign: Campaign
            The entry we are preparing

        Returns
        -------
        steps : dict[str, Step]
            The newly made Steps
        """
        out_dict = {}
        coll_source = campaign.coll_in
        steps = self.config.get("steps", [])
        for step_name in steps:
            step_handler = campaign.get_sub_handler(step_name)
            step_prereqs = step_handler.config.get("prerequisites")
            new_step = step_handler.insert(
                dbi,
                campaign,
                production_name=campaign.p_.name,
                campaign_name=campaign.name,
                step_name=step_name,
                coll_source=coll_source,
            )
            out_dict[step_name] = new_step
            coll_source = new_step.coll_out
            for prereq_step in step_prereqs:
                prereq = dbi.get_entry_from_parent(campaign.db_id, prereq_step)
                Dependency.add_prerequisite(dbi, new_step.db_id, prereq.db_id)
        return out_dict
