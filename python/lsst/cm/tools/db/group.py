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

from typing import Any

from lsst.cm.tools.core.db_interface import CMTableBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.campaign import Campaign
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.script import Script
from lsst.cm.tools.db.step import Step
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import Column, Enum, ForeignKey, String  # type: ignore
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Group(common.Base, common.CMTable):
    __tablename__ = "group"

    level = LevelEnum.group
    id = Column(Integer, primary_key=True)  # Unique Group ID
    p_id = Column(Integer, ForeignKey(Production.id))
    c_id = Column(Integer, ForeignKey(Campaign.id))
    s_id = Column(Integer, ForeignKey(Step.id))
    name = Column(String)  # Group name
    p_name = Column(String)  # Production Name
    c_name = Column(String)  # Campaign Name
    s_name = Column(String)  # Step Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.id))
    collect_script = Column(Integer, ForeignKey(Script.id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    db_id = composite(DbId, p_id, c_id, s_id, id)
    p_ = relationship("Production", foreign_keys=[p_id])
    c_ = relationship("Campaign", foreign_keys=[c_id])
    s_ = relationship("Step", foreign_keys=[s_id])

    match_keys = [p_id, c_id, s_id, id]
    update_fields = common.update_field_list + common.update_common_fields

    @hybrid_property
    def fullname(self):
        return self.p_name + "/" + self.c_name + "/" + self.s_name + "/" + self.name

    @hybrid_property
    def butler_repo(self):
        return self.c_.butler_repo

    @hybrid_property
    def prod_base_url(self):
        return self.c_.prod_base_url

    @classmethod
    def get_parent_key(cls):
        return cls.s_id

    def __repr__(self):
        return f"Group {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        insert_fields = dict(
            name=handler.get_kwarg_value("group_name", **kwargs),
            p_name=handler.get_kwarg_value("production_name", **kwargs),
            c_name=handler.get_kwarg_value("campaign_name", **kwargs),
            s_name=handler.get_kwarg_value("step_name", **kwargs),
            p_id=parent_db_id.p_id,
            c_id=parent_db_id.c_id,
            s_id=parent_db_id.s_id,
            data_query=handler.get_config_var("data_query", "", **kwargs),
            coll_source=handler.get_config_var("coll_source", "", **kwargs),
            status=StatusEnum.waiting,
            handler=handler.get_handler_class_name(),
            config_yaml=handler.config_url,
        )
        extra_fields = dict(
            fullname="{p_name}/{c_name}/{s_name}/{name}".format(**insert_fields),
            prod_base_url=handler.get_kwarg_value("prod_base_url", **kwargs),
        )
        coll_names = handler.coll_name_hook(LevelEnum.group, insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return insert_fields

    @classmethod
    def post_insert(cls, dbi, handler, new_entry: CMTableBase, **kwargs):
        kwcopy = kwargs.copy()
        kwcopy["workflow_idx"] = kwcopy.get("workflow_idx", 0)
        kwcopy.pop('data_query')
        kwcopy.update(coll_source=new_entry.coll_in)
        parent_db_id = dbi.get_db_id(LevelEnum.group, **kwcopy)
        dbi.insert(LevelEnum.workflow, parent_db_id, handler, **kwcopy)
        dbi.prepare(LevelEnum.workflow, parent_db_id)
