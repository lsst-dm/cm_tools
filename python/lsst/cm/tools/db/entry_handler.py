from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface, JobBase, ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import EntryHandlerBase, Handler
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker
from lsst.cm.tools.core.utils import StatusEnum
from lsst.cm.tools.db.common import CMTable
from lsst.cm.tools.db.handler_utils import (
    accept_children,
    accept_entry,
    check_entries,
    check_entry,
    collect_children,
    collect_entry,
    reject_entry,
    rollback_children,
    rollback_entry,
    run_children,
    run_entry,
    supersede_children,
    supersede_entry,
    validate_children,
    validate_entry,
)

# import datetime


class EntryHandler(EntryHandlerBase):
    """Callback handler for database entries

    Provides some interface functions.

    Derived classes will have to:

    1. implement the `xxx_hook` functions.
    2. implement the `insert` and `prepare` functions
    """

    def run(self, dbi: DbInterface, entry: CMTable) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += run_children(dbi, itr)
        db_id_list += run_entry(dbi, self, entry)
        return db_id_list

    def check(self, dbi: DbInterface, entry: CMTable) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += check_entries(dbi, itr)
        db_id_list += check_entry(dbi, entry)
        return db_id_list

    def collect(self, dbi: DbInterface, entry: CMTable) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += collect_children(dbi, itr)
        db_id_list += collect_entry(dbi, self, entry)
        return db_id_list

    def validate(self, dbi: DbInterface, entry: CMTable) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += validate_children(dbi, itr)
        db_id_list += validate_entry(dbi, self, entry)
        return db_id_list

    def accept(self, dbi: DbInterface, entry: CMTable) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += accept_children(dbi, itr)
        db_id_list += accept_entry(dbi, self, entry)
        return db_id_list

    def reject(self, dbi: DbInterface, entry: CMTable) -> list[DbId]:
        return reject_entry(dbi, self, entry)

    def rollback(self, dbi: DbInterface, entry: CMTable, to_status: StatusEnum) -> list[DbId]:
        return rollback_entry(dbi, self, entry, to_status)

    def supersede(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += supersede_children(dbi, itr)
        db_id_list += supersede_entry(dbi, self, entry)
        return db_id_list

    def rollback_subs(self, dbi: DbInterface, entry: CMTable, to_status: StatusEnum) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list = rollback_children(dbi, itr, to_status)
        return db_id_list

    def run_hook(self, dbi: DbInterface, entry: Any) -> list[JobBase]:
        assert entry.status == StatusEnum.prepared
        entry.update_values(dbi, entry.id, status=StatusEnum.running)
        return []

    def accept_hook(self, dbi: DbInterface, entry: Any) -> None:
        pass

    def reject_hook(self, dbi: DbInterface, entry: Any) -> None:
        pass

    def supersede_hook(self, dbi: DbInterface, entry: Any) -> None:
        pass


class GenericEntryHandlerMixin(EntryHandler):
    """Callback handler for database entries

    Provides generic version of interface functions

    """

    yaml_checker_class = YamlChecker().get_checker_class_name()
    rollback_class = FakeRollback().get_rollback_class_name()

    def prepare_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        script_handlers = self.config.get("prepare", {})
        return self._generic_scripts(dbi, entry, script_handlers)

    def collect_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        script_handlers = self.config.get("collect", {})
        return self._generic_scripts(dbi, entry, script_handlers)

    def validate_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        script_handlers = self.config.get("validate", {})
        return self._generic_scripts(dbi, entry, script_handlers)

    @staticmethod
    def _generic_scripts(
        dbi: DbInterface,
        entry: Any,
        script_handlers: dict[str, Any],
    ) -> list[ScriptBase]:
        scripts: list[ScriptBase] = []
        for handler_name, handler_info in script_handlers.items():
            handler_class_name = handler_info.get("class_name", None)
            handler = Handler.get_handler(handler_class_name, entry.config_yaml)
            script = handler.insert(
                dbi,
                entry,
                name=handler_name,
                prepend=f"# Written by {handler.get_handler_class_name()}",
                append="# Have a good day",
                **handler_info,
            )
            status = handler.run(dbi, script)
            if status != StatusEnum.ready:
                script.update_values(dbi, script.id, status=status)
            scripts.append(script)
        return scripts
