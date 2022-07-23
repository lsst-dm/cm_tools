from typing import Any

from lsst.cm.tools.core.db_interface import CMTableBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db import common
from lsst.cm.tools.db.production import Production
from lsst.cm.tools.db.script import Script
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import Column, Enum, ForeignKey, String  # type: ignore
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import composite, relationship


class Campaign(common.Base, common.CMTable):
    __tablename__ = "campaign"

    level = LevelEnum.campaign
    id = Column(Integer, primary_key=True)  # Unique campaign ID
    p_id = Column(Integer, ForeignKey(Production.id))
    name = Column(String)  # Campaign Name
    p_name = Column(String)  # Production Name
    handler = Column(String)  # Handler class
    config_yaml = Column(String)  # Configuration file
    prepare_script = Column(Integer, ForeignKey(Script.id))
    collect_script = Column(Integer, ForeignKey(Script.id))
    data_query = Column(String)  # Data query
    coll_source = Column(String)  # Source data collection
    coll_in = Column(String)  # Input data collection (post-query)
    coll_out = Column(String)  # Output data collection
    status = Column(Enum(StatusEnum))  # Status flag
    butler_repo = Column(String)  # URL for butler repository
    prod_base_url = Column(String)  # URL for root of the production area
    db_id = composite(DbId, p_id, id)
    p_ = relationship("Production", foreign_keys=[p_id])

    match_keys = [p_id, id]
    update_fields = common.update_field_list + common.update_common_fields

    @hybrid_property
    def fullname(self):
        return self.p_name + "/" + self.name

    @classmethod
    def get_parent_key(cls):
        return cls.p_id

    def __repr__(self):
        return f"Campaign {self.fullname} {self.db_id}: {self.handler} {self.config_yaml} {self.status.name}"

    @classmethod
    def get_insert_fields(cls, handler, parent_db_id: DbId, **kwargs) -> dict[str, Any]:
        if "butler_repo" not in kwargs:
            raise KeyError("butler_repo must be specified with inserting a campaign")
        if "prod_base_url" not in kwargs:
            raise KeyError("prod_base_url must be specified with inserting a campaign")
        insert_fields = dict(
            name=handler.get_kwarg_value("campaign_name", **kwargs),
            p_name=handler.get_kwarg_value("production_name", **kwargs),
            coll_source=handler.get_config_var("campaign_coll_source", None, **kwargs),
            data_query=handler.get_config_var("campaign_data_query", None, **kwargs),
            p_id=parent_db_id.p_id,
            status=StatusEnum.waiting,
            butler_repo=kwargs["butler_repo"],
            prod_base_url=kwargs["prod_base_url"],
            handler=handler.get_handler_class_name(),
            config_yaml=handler.config_url,
        )
        extra_fields = dict(
            fullname="{p_name}/{name}".format(**insert_fields),
        )
        coll_names = handler.coll_name_hook(LevelEnum.step, insert_fields, **extra_fields)
        insert_fields.update(**coll_names)
        return insert_fields

    @classmethod
    def post_insert(cls, dbi, handler, new_entry: CMTableBase, **kwargs):
        kwcopy = kwargs.copy()
        previous_step_id = None
        coll_source = new_entry.coll_in
        parent_db_id = dbi.get_db_id(LevelEnum.campaign, **kwcopy)
        for step_name in handler.step_dict.keys():
            kwcopy.update(step_name=step_name)
            kwcopy.update(previous_step_id=previous_step_id)
            kwcopy.update(coll_source=coll_source)
            kwcopy.update(data_query=None)
            step_insert = dbi.insert(LevelEnum.step, parent_db_id, handler, **kwcopy)
            step_id = parent_db_id.extend(LevelEnum.step, step_insert.id)
            coll_source = step_insert.coll_out
            if previous_step_id is not None:
                dbi.add_prerequisite(step_id, parent_db_id.extend(LevelEnum.step, previous_step_id))
            previous_step_id = dbi.get_row_id(LevelEnum.step, **kwcopy)
