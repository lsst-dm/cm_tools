import os
from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import EntryHandlerBase
from lsst.cm.tools.core.script_utils import FakeRollback, YamlChecker
from lsst.cm.tools.core.utils import ScriptType, StatusEnum, safe_makedirs
from lsst.cm.tools.db.common import CMTable
from lsst.cm.tools.db.handler_utils import (
    accept_children,
    accept_entry,
    check_entry_loop,
    reject_entry,
    rollback_children,
    rollback_entry,
    supersede_children,
    supersede_entry,
)
from lsst.cm.tools.db.script import Script

# import datetime


class EntryHandler(EntryHandlerBase):
    """Callback handler for database entries

    Provides some interface functions.

    Derived classes will have to:

    1. implement the `xxx_hook` functions.
    2. implement the `make_children` function
    """

    def prepare(self, dbi: DbInterface, entry: CMTable) -> StatusEnum:
        assert entry.status == StatusEnum.ready
        full_path = os.path.join(entry.prod_base_url, entry.fullname)
        safe_makedirs(full_path)
        handler = entry.get_handler()
        prepare_scripts = handler.prepare_script_hook(dbi, entry)
        if prepare_scripts:
            return StatusEnum.preparing
        return StatusEnum.prepared

    def check(self, dbi: DbInterface, entry: CMTable) -> StatusEnum:
        return check_entry_loop(dbi, entry)

    def collect(self, dbi: DbInterface, entry: CMTable) -> StatusEnum:
        assert entry.status == StatusEnum.collectable
        collect_scripts = self.collect_script_hook(dbi, entry)
        if collect_scripts:
            return StatusEnum.collecting
        return StatusEnum.completed

    # revisit this wrt rescuable status if we run validate on
    # workflows rather than groups
    def validate(self, dbi: DbInterface, entry: CMTable) -> StatusEnum:
        assert entry.status == StatusEnum.completed
        validate_scripts = self.validate_script_hook(dbi, entry)
        if validate_scripts:
            return StatusEnum.validating
        return StatusEnum.accepted

    def accept(self, dbi: DbInterface, entry: CMTable, rescuable: bool = False) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += accept_children(dbi, itr, rescuable)
        db_id_list += accept_entry(dbi, self, entry, rescuable)
        return db_id_list

    def reject(self, dbi: DbInterface, entry: CMTable, purge: bool = False) -> list[DbId]:
        return reject_entry(dbi, self, entry, purge)

    def rollback(
        self, dbi: DbInterface, entry: CMTable, to_status: StatusEnum, purge: bool = False
    ) -> StatusEnum:
        return rollback_entry(dbi, self, entry, to_status, purge)

    def supersede(self, dbi: DbInterface, entry: Any, purge: bool = False) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list += supersede_children(dbi, itr, purge)
        db_id_list += supersede_entry(dbi, self, entry, purge)
        return db_id_list

    def rollback_subs(
        self, dbi: DbInterface, entry: CMTable, to_status: StatusEnum, purge: bool = False
    ) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list = rollback_children(dbi, itr, to_status, purge)
        return db_id_list

    def accept_hook(self, dbi: DbInterface, entry: Any) -> None:
        pass

    def reject_hook(self, dbi: DbInterface, entry: Any, purge: bool = False) -> None:
        pass

    def supersede_hook(self, dbi: DbInterface, entry: Any, purge: bool = False) -> None:
        pass

    def _make_jobs(self, dbi: DbInterface, entry: Any) -> None:
        pass


class GenericEntryHandler(EntryHandler):
    """Callback handler for database entries

    Provides generic version of interface functions

    """

    yaml_checker_class = YamlChecker().get_checker_class_name()
    rollback_class = FakeRollback().get_rollback_class_name()

    def make_scripts(self, dbi: DbInterface, entry: Any) -> None:
        script_handlers = self.config.get("scripts", [])
        self._insert_generic_scripts(dbi, entry, script_handlers)
        self._make_jobs(dbi, entry)
        return StatusEnum.ready

    def prepare_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        script_handlers = self.config.get("scripts", [])
        return self._run_generic_scripts(dbi, entry, script_handlers, ScriptType.prepare)

    def collect_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        script_handlers = self.config.get("scripts", [])
        return self._run_generic_scripts(dbi, entry, script_handlers, ScriptType.collect)

    def validate_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        script_handlers = self.config.get("scripts", [])
        return self._run_generic_scripts(dbi, entry, script_handlers, ScriptType.validate)

    def rerun_script(self, dbi: DbInterface, entry: Any, script_name: str, script_type: ScriptType) -> None:
        self._insert_generic_scripts(dbi, entry, [script_name])
        self._run_generic_scripts(dbi, entry, [script_name], script_type)

    @staticmethod
    def _insert_generic_scripts(
        dbi: DbInterface,
        entry: Any,
        script_handlers: list[str],
    ) -> list[ScriptBase]:
        scripts: list[ScriptBase] = []
        for handler_name in script_handlers:
            fragment = entry.config_.get_fragment(handler_name)
            handler = fragment.get_handler()
            script = handler.insert(
                dbi,
                entry,
                name=handler_name,
                **fragment.data,
            )
            scripts.append(script)
        return scripts

    @staticmethod
    def _run_generic_scripts(
        dbi: DbInterface,
        entry: Any,
        script_handlers: list[str],
        script_type: ScriptType,
    ) -> list[ScriptBase]:
        scripts: list[ScriptBase] = []
        for handler_name in script_handlers:
            fragment = entry.config_.get_fragment(handler_name)
            for script_ in entry.scripts_:
                if script_.name != handler_name:
                    continue
                if script_.script_type != script_type:
                    continue
                if script_.superseded:
                    continue
                handler = script_.get_handler()
                status = handler.run(
                    dbi,
                    entry,
                    script_,
                    prepend=f"#!/bin/sh\n\n# Written by {handler.get_handler_class_name()}",
                    append="# Have a good day",
                    **fragment.data,
                )
                Script.update_values(dbi, script_.id, status=status)
                scripts.append(script_)
        return scripts
