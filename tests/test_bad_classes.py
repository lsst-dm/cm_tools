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

import sys

import pytest
from lsst.cm.tools.core.db_interface import DbId, DbInterface
from lsst.cm.tools.core.grouper import Grouper
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum


def test_bad_db_interface():

    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()
    null_db_id = DbId()

    with pytest.raises(NotImplementedError):
        BadDbInterface.full_name(LevelEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.get_db_id(LevelEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.get_row_id(LevelEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.get_status(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.print_(sys.stdout, LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.print_table(sys.stdout, LevelEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.count(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.update(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.check(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.get_data(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.get_iterable(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.insert(LevelEnum.production, null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_db.prepare(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.queue_workflows(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.launch_workflows(LevelEnum.production, null_db_id, 100)

    with pytest.raises(NotImplementedError):
        bad_db.accept(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.reject(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.fake_run(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.daemon(null_db_id)


def test_bad_grouper():

    class BadDbInterface(DbInterface):
        pass

    class BadGrouper(Grouper):
        pass

    bad_db = BadDbInterface()
    null_db_id = DbId()
    bad_grouper = BadGrouper()

    with pytest.raises(NotImplementedError):
        bad_grouper({}, bad_db, null_db_id, None)


def test_bad_handler():

    class BadDbInterface(DbInterface):
        pass

    class BadHandler(Handler):

        @classmethod
        def bad_get_kwarg(cls):
            return cls._get_kwarg_value('bad')

        def bad_resolve_templated(self):
            self._config['bad_template'] = '{missing}'
            self._resolve_templated_string('bad_template', {})

    bad_db = BadDbInterface()
    null_db_id = DbId()
    bad_handler = BadHandler()

    with pytest.raises(KeyError):
        BadHandler.bad_get_kwarg()

    with pytest.raises(KeyError):
        bad_handler.bad_resolve_templated()

    with pytest.raises(NotImplementedError):
        bad_handler.get_insert_fields_hook(LevelEnum.production, bad_db, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_handler.post_insert_hook(LevelEnum.production, bad_db, {})

    with pytest.raises(NotImplementedError):
        bad_handler.get_update_fields_hook(LevelEnum.production, bad_db, None, [])

    with pytest.raises(NotImplementedError):
        bad_handler.prepare_hook(LevelEnum.production, bad_db, null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_handler.prepare_script_hook(LevelEnum.production, bad_db, null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_handler.launch_workflow_hook(bad_db, null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_handler.check_workflow_status_hook(bad_db, null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_handler.collection_hook(LevelEnum.production, bad_db, null_db_id, [], None)

    with pytest.raises(NotImplementedError):
        bad_handler.accept_hook(LevelEnum.production, bad_db, null_db_id, [], None)

    with pytest.raises(NotImplementedError):
        bad_handler.reject_hook(LevelEnum.production, bad_db, null_db_id, [])

    with pytest.raises(NotImplementedError):
        bad_handler.fake_run_hook(bad_db, null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_handler.check_script_status_hook("")
