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

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase
from lsst.cm.tools.core.utils import StatusEnum
from lsst.cm.tools.db import common
from sqlalchemy import Integer  # type: ignore
from sqlalchemy import Column, Enum, String, func, select, update  # type: ignore


class Script(common.Base, ScriptBase):
    __tablename__ = "script"

    id = Column(Integer, primary_key=True)  # Unique script ID
    script_url = Column(String)  # Url for script
    log_url = Column(String)  # Url for log
    config_url = Column(String)  # Url for config
    checker = Column(String)  # Checker class
    status = Column(Enum(StatusEnum))  # Status flag

    def __repr__(self):
        return f"Script {self.id}: {self.checker} {self.log_url} {self.status.name}"

    def check_status(self, dbi: DbInterface) -> StatusEnum:
        current_status = self.status
        checker = Checker.get_checker(self.checker)
        new_status = checker.check_url(self.log_url, self.status)
        if new_status != current_status:
            stmt = update(Script).where(Script.id == self.id).values(status=new_status)
            conn = dbi.connection()
            upd_result = conn.execute(stmt)
            common.check_result(upd_result)
            conn.commit()
        return new_status

    @classmethod
    def add_script(cls, dbi: DbInterface, **kwargs) -> ScriptBase:
        """Insert a new row with details about a script"""
        counter = func.count(cls.id)
        next_id = common.return_count(dbi, counter) + 1
        script = cls(id=next_id, **kwargs)
        conn = dbi.connection()
        conn.add(script)
        conn.commit()
        return script

    @classmethod
    def get_script(cls, dbi: DbInterface, script_id: int) -> ScriptBase:
        """Get a particular script by id"""
        sel = select(Script).where(Script.id == script_id)
        return common.return_single_row(dbi, sel)[0]
