import sys
from typing import Any

import pytest
from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import CMTableBase, DbId, DbInterface, DependencyBase, ScriptBase
from lsst.cm.tools.core.handler import EntryHandlerBase, Handler, JobHandlerBase, ScriptHandlerBase
from lsst.cm.tools.core.rollback import Rollback
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db.group_handler import GroupHandler
from lsst.cm.tools.db.script_handler import ScriptHandler
from lsst.cm.tools.db.step_handler import StepHandler
from lsst.cm.tools.db.workflow_handler import WorkflowHandler


def test_bad_script() -> None:
    class BadScript(ScriptBase):
        pass

    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()

    with pytest.raises(NotImplementedError):
        BadScript.insert_values(bad_db)

    with pytest.raises(NotImplementedError):
        BadScript.update_values(bad_db, 0)

    with pytest.raises(NotImplementedError):
        BadScript.check_status(bad_db, None)

    with pytest.raises(NotImplementedError):
        BadScript.rollback_script(bad_db, None, None)


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
        bad_db.get_entry_from_fullname(LevelEnum.production, "")

    with pytest.raises(NotImplementedError):
        bad_db.get_entry_from_parent(null_db_id, "")

    with pytest.raises(NotImplementedError):
        bad_db.print_(sys.stdout, TableEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.print_table(sys.stdout, TableEnum.production)

    with pytest.raises(NotImplementedError):
        bad_db.print_tree(sys.stdout, TableEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.check(TableEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.insert(null_db_id, None)

    with pytest.raises(NotImplementedError):
        bad_db.prepare(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.queue_jobs(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.launch_jobs(LevelEnum.production, null_db_id, 100)

    with pytest.raises(NotImplementedError):
        bad_db.accept(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.reject(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.rollback(LevelEnum.production, null_db_id, StatusEnum.waiting)

    with pytest.raises(NotImplementedError):
        bad_db.fake_run(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.supersede(LevelEnum.production, null_db_id)

    with pytest.raises(NotImplementedError):
        bad_db.daemon(null_db_id)


def test_bad_checker() -> None:
    class BadChecker(Checker):
        pass

    bad_checker = BadChecker()

    with pytest.raises(NotImplementedError):
        bad_checker.check_url(None, StatusEnum.ready)

    with pytest.raises(TypeError):
        Checker.get_checker("lsst.cm.tools.core")


def test_bad_rollback() -> None:
    class BadRollback(Rollback):
        pass

    bad_rollback = BadRollback()

    with pytest.raises(NotImplementedError):
        bad_rollback.rollback_script(None, None)


def test_bad_handler() -> None:
    class BadDbInterface(DbInterface):
        pass

    bad_db = BadDbInterface()

    class BadHandler(Handler):
        @classmethod
        def bad_get_kwarg(cls) -> Any:
            return cls.get_kwarg_value("bad")

        def bad_resolve_templated(self) -> None:
            self.config["bad_template"] = "{missing}"
            self.resolve_templated_string("bad_template")

    with pytest.raises(TypeError):
        Handler.get_handler("lsst.cm.tools.core.handler", "dummy.yaml")

    with pytest.raises(KeyError):
        BadHandler.bad_get_kwarg()

    class BadScriptHandlerBase(ScriptHandlerBase):
        pass

    bad_script_handler_base = BadScriptHandlerBase()

    with pytest.raises(NotImplementedError):
        bad_script_handler_base.insert(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_script_handler_base.write_script_hook(bad_db, None, None)

    with pytest.raises(NotImplementedError):
        bad_script_handler_base.fake_run_hook(bad_db, None, StatusEnum.waiting)

    with pytest.raises(NotImplementedError):
        bad_script_handler_base.run(bad_db, None)

    class BadJobHandlerBase(JobHandlerBase):
        pass

    bad_job_handler_base = BadJobHandlerBase()

    with pytest.raises(NotImplementedError):
        bad_job_handler_base.insert(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_job_handler_base.write_job_hook(bad_db, None, None)

    with pytest.raises(NotImplementedError):
        bad_job_handler_base.fake_run_hook(bad_db, None, StatusEnum.waiting)

    with pytest.raises(NotImplementedError):
        bad_job_handler_base.launch(bad_db, None)

    class BadScriptHandler(ScriptHandler):
        pass

    bad_script_handler = BadScriptHandler()

    with pytest.raises(NotImplementedError):
        bad_script_handler.get_coll_out_name(None)

    class BadEntryHandler(EntryHandlerBase):
        pass

    bad_entry_handler = BadEntryHandler()

    with pytest.raises(NotImplementedError):
        bad_entry_handler.insert(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.prepare(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.check(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.collect(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.validate(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.accept(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.reject(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.supersede(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.run(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.rollback(bad_db, None, StatusEnum.waiting)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.prepare_script_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.collect_script_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.validate_script_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.accept_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.run_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.reject_hook(bad_db, None)

    with pytest.raises(NotImplementedError):
        bad_entry_handler.supersede_hook(bad_db, None)

    class BadGroupHandler(GroupHandler):
        pass

    bad_group_handler = BadGroupHandler()

    with pytest.raises(NotImplementedError):
        bad_group_handler.make_workflow_handler()

    class BadStepHandler(StepHandler):
        pass

    bad_step_handler = BadStepHandler()

    with pytest.raises(NotImplementedError):
        bad_step_handler.group_iterator(bad_db, None)

    class BadWorkflowHandler(WorkflowHandler):
        pass

    bad_workflow_handler = BadWorkflowHandler()

    with pytest.raises(NotImplementedError):
        bad_workflow_handler.make_job_handler()
