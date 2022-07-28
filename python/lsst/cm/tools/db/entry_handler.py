from typing import Any

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import EntryHandlerBase
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
        db_id_list += accept_entry(dbi, entry)
        return db_id_list

    def reject(self, dbi: DbInterface, entry: CMTable) -> list[DbId]:
        return reject_entry(dbi, entry)

    def rollback(self, dbi: DbInterface, entry: CMTable, to_status: StatusEnum) -> list[DbId]:
        return rollback_entry(dbi, self, entry, to_status)

    def rollback_run(self, dbi: DbInterface, entry: CMTable, to_status: StatusEnum) -> list[DbId]:
        db_id_list: list[DbId] = []
        for itr in entry.sub_iterators():
            db_id_list = rollback_children(dbi, itr, to_status)
        return db_id_list

    def run_hook(self, dbi: DbInterface, entry: Any) -> list[JobBase]:
        current_status = entry.status
        if current_status != StatusEnum.prepared:
            return []
        entry.update_values(dbi, entry.id, status=StatusEnum.running)
        return []
