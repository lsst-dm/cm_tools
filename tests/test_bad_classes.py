import sys
from typing import Any

import pytest
from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import (
    CMTableBase,
    DbId,
    DbInterface,
    DependencyBase,
    ScriptBase,
    WorkflowBase,
)
from lsst.cm.tools.core.grouper import Grouper
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum


def test_bad_script() -> None:
    class BadScript(ScriptBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()

    with pytest.raises(NotImplementedError):
        BadScript.insert_values(bad_db)

    with pytest.raises(NotImplementedError):
        BadScript.get(bad_db, 0)

    with pytest.raises(NotImplementedError):
        BadScript.update_values(bad_db, 0)

    with pytest.raises(NotImplementedError):
        BadScript.check_status(bad_db, None)

    with pytest.raises(NotImplementedError):
        BadScript.rollback_script(bad_db, None)


def test_bad_workflow() -> None:
    class BadWorkflow(WorkflowBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()

    with pytest.raises(NotImplementedError):
        BadWorkflow.insert_values(bad_db)

    with pytest.raises(NotImplementedError):
        BadWorkflow.get(bad_db, 0)

    with pytest.raises(NotImplementedError):
        BadWorkflow.update_values(bad_db, 0)

    with pytest.raises(NotImplementedError):
        BadWorkflow.check_status(bad_db, None)

    with pytest.raises(NotImplementedError):
        BadWorkflow.rollback_script(bad_db, None)


def test_bad_dependency() -> None:
    class BadDependency(DependencyBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()
    null_db_id = DbId()

    with pytest.raises(NotImplementedError):
        BadDependency.add_prerequisite(bad_db, null_db_id, null_db_id)


def test_bad_cm_table() -> None:
    class BadCMTableBase(CMTableBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_cm_table_base = BadCMTableBase()

    with pytest.raises(NotImplementedError):
        bad_cm_table_base.get_handler()


def test_bad_db_interface() -> None:
    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()
    null_db_id = DbId()

    with pytest.raises(NotImplementedError):
        bad_db.connection()

    with pytest.raises(NotImplementedError):
        bad_db.get_db_id(LevelEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.get_entry(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.get_script(0)

    with pytest.raises(NotImplementedError):
        bad_db.get_workflow(0)

    with pytest.raises(NotImplementedError):
        bad_db.print_(sys.stdout, TableEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.print_table(sys.stdout, TableEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.count(TableEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.update(TableEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.check(TableEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.add_prerequisite(null_db_id, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.add_script()

    with pytest.raises(NotImplementedError):
        bad_db.insert(null_db_id, None)

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


def test_bad_grouper() -> None:
    class BadDbInterface(DbInterface):
        pass

    class BadGrouper(Grouper):
        pass

    bad_db = BadDbInterface()
    null_db_id = DbId()
    bad_grouper = BadGrouper()

    with pytest.raises(NotImplementedError):
        bad_grouper({}, bad_db, null_db_id, None)


def test_bad_checker() -> None:
    class BadChecker(Checker):
        pass

    bad_checker = BadChecker()

    with pytest.raises(NotImplementedError):
        bad_checker.check_url(None, StatusEnum.ready)


def test_bad_handler() -> None:
    class BadDbInterface(DbInterface):
        pass

    class BadHandler(Handler):
        @classmethod
        def bad_get_kwarg(cls) -> Any:
            return cls.get_kwarg_value("bad")

        def bad_resolve_templated(self) -> None:
            self.config["bad_template"] = "{missing}"
            self.resolve_templated_string("bad_template")

    bad_handler = BadHandler()

    with pytest.raises(KeyError):
        BadHandler.bad_get_kwarg()

    with pytest.raises(KeyError):
        bad_handler.bad_resolve_templated()
