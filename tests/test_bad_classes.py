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
from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import CMTableBase, DbId, DbInterface, DependencyBase, ScriptBase
from lsst.cm.tools.core.grouper import Grouper
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.common import CMTable


def test_bad_script():
    class BadScript(ScriptBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()
    bad_script = BadScript()

    with pytest.raises(NotImplementedError):
        bad_script.check_status(None)

    with pytest.raises(NotImplementedError):
        bad_script.add_script(bad_db)

    with pytest.raises(NotImplementedError):
        bad_script.get_script(bad_db, 0)


def test_bad_dependency():
    class BadDependency(DependencyBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()
    bad_dep = BadDependency()
    null_db_id = DbId()

    with pytest.raises(NotImplementedError):
        bad_dep.add_prerequisite(bad_db, null_db_id, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_dep.get_prerequisites(bad_db, null_db_id)


def test_bad_cm_table():
    class BadCMTableBase(CMTableBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_cm_table_base = BadCMTableBase()
    bad_db = BadDbInterface()
    null_db_id = DbId()

    with pytest.raises(NotImplementedError):
        bad_cm_table_base.get_handler()

    with pytest.raises(NotImplementedError):
        bad_cm_table_base.get_insert_fields(None, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_cm_table_base.post_insert(bad_db, None, {})

    with pytest.raises(NotImplementedError):
        CMTable.get_parent_key()


def test_bad_db_interface():
    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()
    null_db_id = DbId()

    with pytest.raises(NotImplementedError):
        bad_db.connection()

    with pytest.raises(NotImplementedError):
        bad_db.get_repo(null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.get_prod_base(null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.get_db_id(LevelEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.get_row_id(LevelEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.get_status(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.get_prerequisites(null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.get_script(0)

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
        bad_db.add_prerequisite(null_db_id, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.add_script()

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


def test_bad_checker():
    class BadChecker(Checker):
        pass

    bad_checker = BadChecker()

    with pytest.raises(NotImplementedError):
        bad_checker.check_url(None, StatusEnum.ready)


def test_bad_handler():
    class BadDbInterface(DbInterface):
        pass

    class BadHandler(Handler):
        @classmethod
        def bad_get_kwarg(cls):
            return cls.get_kwarg_value("bad")

        def bad_resolve_templated(self):
            self.config["bad_template"] = "{missing}"
            self.resolve_templated_string("bad_template", {})

    bad_db = BadDbInterface()
    null_db_id = DbId()
    bad_handler = BadHandler()

    with pytest.raises(KeyError):
        BadHandler.bad_get_kwarg()

    with pytest.raises(KeyError):
        bad_handler.bad_resolve_templated()

    with pytest.raises(NotImplementedError):
        bad_handler.coll_name_hook(LevelEnum.production, {})

    with pytest.raises(NotImplementedError):
        bad_handler.prepare_script_hook(LevelEnum.production, bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_handler.workflow_script_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_handler.check_workflow_status_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_handler.collect_script_hook(LevelEnum.production, bad_db, [], None)

    with pytest.raises(NotImplementedError):
        bad_handler.accept_hook(LevelEnum.production, bad_db, [], None)

    with pytest.raises(NotImplementedError):
        bad_handler.reject_hook(LevelEnum.production, bad_db, [])

    with pytest.raises(NotImplementedError):
        bad_handler.fake_run_hook(bad_db, null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_handler.check_prerequistes(bad_db, null_db_id)
