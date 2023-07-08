import os
from typing import Any, Iterable, Optional

import numpy as np
from lsst.daf.butler import Butler

from lsst.cm.tools.core.butler_utils import build_data_queries, fake_data_queries
from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.handler import Handler
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
        # coll_source = self.get_kwarg_value("coll_source", **kwargs)
        coll_source = kwargs.get("coll_source", parent.coll_in)
        insert_fields = dict(
            name=step_name,
            fullname=self.get_fullname(**kwargs),
            p_id=parent.p_.id,
            c_id=parent.id,
            config_id=parent.config_id,
            frag_id=self._fragment_id,
            data_query=kwargs.get("data_query"),
            coll_in=coll_source,
            coll_source=coll_source,
            bps_yaml_template=self.get_config_var("bps_yaml_template", parent.bps_yaml_template, **kwargs),
            bps_script_template=self.get_config_var(
                "bps_script_template", parent.bps_script_template, **kwargs
            ),
            lsst_version=self.get_config_var("lsst_version", parent.lsst_version, **kwargs),
            lsst_custom_setup=self.get_config_var("lsst_custom_setup", parent.lsst_custom_setup, **kwargs),
            pipeline_yaml=self.get_config_var("pipeline_yaml", None, **kwargs),
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
        group_config_block = self.get_config_var("group_config", "group")
        group_handler = entry.get_sub_handler(group_config_block)
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
        out_dict = dict(
            production_name=entry.p_.name,
            campaign_name=entry.c_.name,
            step_name=entry.name,
        )
        data_query_base = self.config.get("data_query_base", "")
        split_args = self.config.get("split_args", {})
        split_vals = self.config.get("split_vals", {})
        split_dict = self.config.get("split_dict", {})
        if split_args:
            butler = Butler(
                entry.butler_repo,
                collections=[entry.coll_source],
            )
            if Handler.script_method.value > 0:
                data_queries = build_data_queries(butler, **split_args)
            else:
                data_queries = fake_data_queries(
                    field=split_args.get("field"), min_queries=split_args.get("min_queries")
                )
        elif split_vals:
            split_field = split_vals["field"]
            split_list = split_vals["values"]
            data_queries = [f"{split_field} in ({split_value_})" for split_value_ in split_list]
        elif split_dict:
            split_field = split_dict["field"]
            id_dict = split_dict["id_dict"]
            data_queries = []
            for key, val in id_dict.items():
                id_list = np.loadtxt(os.path.expandvars(val["id_file"])).astype("int")
                id_per_query = val["id_per_query"]
                chunks = np.split(id_list, range(id_per_query, len(id_list), id_per_query))
                data_queries += [
                    f"{split_field} in ( {', '.join(chunk_.astype('str'))} )" for chunk_ in chunks
                ]
        else:
            data_queries = [None]
        for i, dq_ in enumerate(data_queries):
            data_query = data_query_base
            if dq_ is not None:
                data_query += f" AND {dq_}"
            out_dict.update(
                group_name=f"group{i}",
                data_query=data_query,
            )
            yield out_dict
