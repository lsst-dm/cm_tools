"""Base class to make handlers for a particular Production"""

from lsst.cm.tools.core.utils import level_name, LevelEnum
from lsst.cm.tools.core.handler import Handler


class ProductionHandler(Handler):

    name = 'ProductionHandler'

    def _insert(self, level: LevelEnum, db, **kwargs):
        print(f"create called at {level_name(level)} on {db} with: ", kwargs)
        return kwargs

    def _update(self, level: LevelEnum, db, data, itr, **kwargs):
        print(f"update called at {level_name(level)} on {db} with: ", kwargs)
        return kwargs
