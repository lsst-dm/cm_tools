# This file is part of cm_tools
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
from collections import OrderedDict
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.dependency import Dependency
from lsst.cm.tools.db.entry_handler import EntryHandler
from lsst.cm.tools.db.handler_utils import prepare_entry
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.step import Step

# import datetime


class CampaignHandler(EntryHandler):
    """Campaign level callback handler

    Provides interface functions.

    Derived classes will have to:

    1. implement the `xxx_hook` functions.
    2. provide a mapping for Step-level callback handlers in `step_dict`
    """

    config_block = "campaign"

    fullname_template = os.path.join("{production_name}", "{campaign_name}")

    level = LevelEnum.campaign

    step_dict: OrderedDict[str, tuple[type, list[str]]] = OrderedDict([])

    def insert(self, dbi: DbInterface, parent: Production, **kwargs: Any) -> Campaign:
        if "butler_repo" not in kwargs:
            raise KeyError("butler_repo must be specified with inserting a campaign")
        if "prod_base_url" not in kwargs:
            raise KeyError("prod_base_url must be specified with inserting a campaign")
        insert_fields = dict(
            name=self.get_kwarg_value("campaign_name", **kwargs),
            fullname=self.get_fullname(**kwargs),
            data_query=self.get_config_var("data_query", None, **kwargs),
            root_coll=self.get_config_var("root_coll", "prod", **kwargs),
            p_id=parent.id,
            status=StatusEnum.ready,
            butler_repo=kwargs["butler_repo"],
            prod_base_url=kwargs["prod_base_url"],
            handler=self.get_handler_class_name(),
            config_yaml=self.config_url,
        )
        coll_names = self.coll_names(insert_fields)
        insert_fields.update(**coll_names)
        campaign = Campaign.insert_values(dbi, **insert_fields)
        return campaign

    def make_children(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        self.make_steps(dbi, entry)
        return self.prepare_children(dbi, entry)

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
        for step_name, (step_handler_class, step_prereqs) in self.step_dict.items():
            step_handler = Handler.get_handler(
                step_handler_class().get_handler_class_name(),
                campaign.config_yaml,
            )
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

    def prepare(self, dbi: DbInterface, entry: Campaign) -> list[DbId]:
        return prepare_entry(dbi, self, entry)