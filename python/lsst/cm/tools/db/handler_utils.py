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

import os
from typing import Any, Iterable

import numpy as np
from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, ScriptType, StatusEnum, safe_makedirs
from lsst.cm.tools.db.script import Script
from lsst.cm.tools.db.workflow import Workflow

prepare_script_status_map = {
    StatusEnum.failed: StatusEnum.failed,
    StatusEnum.ready: StatusEnum.preparing,
    StatusEnum.running: StatusEnum.preparing,
    StatusEnum.completed: StatusEnum.prepared,
    StatusEnum.accepted: StatusEnum.prepared,
}

collect_script_status_map = {
    StatusEnum.failed: StatusEnum.failed,
    StatusEnum.ready: StatusEnum.collecting,
    StatusEnum.running: StatusEnum.collecting,
    StatusEnum.completed: StatusEnum.completed,
    StatusEnum.accepted: StatusEnum.completed,
}


validate_script_status_map = {
    StatusEnum.failed: StatusEnum.failed,
    StatusEnum.rejected: StatusEnum.rejected,
    StatusEnum.ready: StatusEnum.validating,
    StatusEnum.running: StatusEnum.validating,
    StatusEnum.completed: StatusEnum.reviewable,
    StatusEnum.accepted: StatusEnum.accepted,
}


workflow_status_map = {
    StatusEnum.failed: StatusEnum.failed,
    StatusEnum.ready: StatusEnum.running,
    StatusEnum.pending: StatusEnum.running,
    StatusEnum.running: StatusEnum.running,
    StatusEnum.completed: StatusEnum.collectable,
}


def extract_child_status(itr: Iterable) -> np.ndarray:
    """Return the status of all children in an array"""
    return np.array([x.status.value for x in itr if not x.superseeded])


def extract_scripts_status(itr: Iterable, script_type: ScriptType) -> np.ndarray:
    """Return the status of all children in an array"""
    return np.array([x.status.value for x in itr if (x.script_type == script_type) and not x.superseeded])


def extract_workflow_status(itr: Iterable) -> np.ndarray:
    """Return the status of all children in an array"""
    return np.array([x.status.value for x in itr])


def check_children(entry: Any, min_status: StatusEnum, max_status: StatusEnum) -> StatusEnum:
    """Check the status of childern of a given row
    and return a status accordingly"""
    child_status = extract_child_status(entry.children())
    if child_status.size and (child_status >= StatusEnum.accepted.value).all():
        return StatusEnum.collectable
    status_val = min(max_status.value, max(min_status.value, child_status.min()))
    return StatusEnum(status_val)


def check_scripts(dbi: DbInterface, scripts: Iterable, script_type: ScriptType) -> StatusEnum:
    """Check the status all the scripts of a given type"""
    for script in scripts:
        if script.script_type != script_type:
            continue
        Script.check_status(dbi, script)
    scripts_status = extract_scripts_status(scripts, script_type)
    if not scripts_status.size:
        # No scripts to check, return completed
        return StatusEnum.accepted
    if (scripts_status >= StatusEnum.accepted.value).all():
        return StatusEnum.accepted
    if (scripts_status >= StatusEnum.completed.value).all():
        return StatusEnum.completed
    if (scripts_status < 0).any():
        return StatusEnum.failed
    if (scripts_status >= StatusEnum.running.value).any():
        return StatusEnum.running
    return StatusEnum.ready


def check_workflows(dbi: DbInterface, workflows: Iterable) -> StatusEnum:
    for workflow in workflows:
        Workflow.check_status(dbi, workflow)
    workflow_status = extract_workflow_status(workflows)
    assert workflow_status.size
    if (workflow_status >= StatusEnum.completed.value).all():
        return StatusEnum.completed
    if (workflow_status < 0).any():
        return StatusEnum.failed
    if (workflow_status >= StatusEnum.running.value).any():
        return StatusEnum.running
    return StatusEnum.ready


def check_prepare_scripts(dbi: DbInterface, entry: Any) -> StatusEnum:
    """Check the status prepare scripts for one entry"""
    script_status = check_scripts(dbi, entry.scripts_, ScriptType.prepare)
    return prepare_script_status_map[script_status]


def check_collect_scripts(dbi: DbInterface, entry: Any) -> StatusEnum:
    """Check the status collect scripts for one entry"""
    script_status = check_scripts(dbi, entry.scripts_, ScriptType.collect)
    return collect_script_status_map[script_status]


def check_validation_scripts(dbi: DbInterface, entry: Any) -> StatusEnum:
    """Check the status collect scripts for one entry"""
    script_status = check_scripts(dbi, entry.scripts_, ScriptType.validate)
    return validate_script_status_map[script_status]


def check_workflows_for_entry(dbi: DbInterface, entry: Any) -> StatusEnum:
    """Check the status the workflow for one entry"""
    workflow_status = check_workflows(dbi, entry.w_)
    return workflow_status_map[workflow_status]


def check_entry(dbi: DbInterface, entry: Any) -> list[DbId]:
    current_status = entry.status
    new_status = current_status

    can_continue = True
    db_id_list = []
    while can_continue:
        if current_status.bad() or current_status == StatusEnum.accepted:
            break
        if current_status in [
            StatusEnum.reviewable,
        ]:
            # These require external input, so break
            break
        handler = entry.get_handler()
        if current_status == StatusEnum.waiting:
            if entry.check_prerequistes(dbi):
                new_status = StatusEnum.ready
        elif current_status == StatusEnum.ready:
            handler.prepare(dbi, entry)
            new_status = StatusEnum.preparing
        elif current_status == StatusEnum.preparing:
            new_status = check_prepare_scripts(dbi, entry)
        elif current_status in [StatusEnum.prepared, StatusEnum.pending, StatusEnum.running]:
            if entry.level == LevelEnum.group:
                new_status = check_workflows_for_entry(dbi, entry)
            else:
                new_status = check_children(entry, StatusEnum.pending, StatusEnum.collectable)
        elif current_status == StatusEnum.collectable:
            handler.collect(dbi, entry)
            new_status = StatusEnum.collecting
        elif current_status == StatusEnum.collecting:
            new_status = check_collect_scripts(dbi, entry)
        elif current_status == StatusEnum.completed:
            handler.validate(dbi, entry)
            new_status = StatusEnum.validating
        elif current_status == StatusEnum.validating:
            new_status = check_validation_scripts(dbi, entry)
        if current_status != new_status:
            db_id_list += [entry.db_id]
            entry.update_values(dbi, entry.id, status=new_status)
            current_status = new_status
        else:
            can_continue = False
        if current_status in [StatusEnum.reviewable, StatusEnum.accepted]:
            can_continue = False
    return db_id_list


def check_entries(dbi: DbInterface, itr: Iterable) -> list[DbId]:
    db_id_list = []
    for entry in itr:
        db_id_list += check_entry(dbi, entry)
    return db_id_list


def prepare_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    if entry.status == StatusEnum.waiting:
        if not entry.check_prerequistes(dbi):
            return []
    elif entry.status != StatusEnum.ready:
        return []
    full_path = os.path.join(entry.prod_base_url, entry.fullname)
    safe_makedirs(full_path)
    prepare_scripts = handler.prepare_script_hook(dbi, entry)
    if prepare_scripts:
        status = StatusEnum.preparing
    else:
        status = StatusEnum.prepared
    entry.update_values(dbi, entry.id, status=status)
    return [entry.db_id]


def collect_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    assert entry.status == StatusEnum.collectable
    collect_scripts = handler.collect_script_hook(dbi, entry)
    if collect_scripts:
        status = StatusEnum.collecting
    else:
        status = StatusEnum.completed
    entry.update_values(dbi, entry.id, status=status)
    return [entry.db_id]


def collect_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    db_id_list: list[DbId] = []
    for child in children:
        if child.status != StatusEnum.collectable:
            continue
        handler = child.get_handler()
        db_id_list += collect_entry(dbi, handler, child)
    return db_id_list


def validate_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    assert entry.status == StatusEnum.completed
    validate_scripts = handler.validate_script_hook(dbi, entry)
    if validate_scripts:
        status = StatusEnum.validating
    else:
        status = StatusEnum.accepted
    entry.update_values(dbi, entry.id, status=status)
    return [entry.db_id]


def validate_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    db_id_list: list[DbId] = []
    for child in children:
        if child.status != StatusEnum.completed:
            continue
        handler = child.get_handler()
        db_id_list += validate_entry(dbi, handler, child)
    return db_id_list


def accept_scripts(dbi: DbInterface, scripts: Iterable) -> None:
    for script in scripts:
        assert script.status == StatusEnum.completed
        script.update_values(dbi, script.id, status=StatusEnum.accepted)


def accept_entry(dbi: DbInterface, entry: Any) -> list[DbId]:
    db_id_list: list[DbId] = []
    if entry.status != StatusEnum.reviewable:
        return db_id_list
    accept_scripts(dbi, entry.scripts_)
    db_id_list += [entry.db_id]
    entry.update_values(dbi, entry.id, status=StatusEnum.accepted)
    return db_id_list


def accept_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    db_id_list = []
    for child in children:
        handler = child.get_handler()
        db_id_list += handler.accept(dbi, child)
    return db_id_list


def reject_entry(dbi: DbInterface, entry: Any) -> list[DbId]:
    if entry.status == StatusEnum.accepted:
        raise ValueError("Rejecting an already accepted entry {entry.db_id}")
    entry.update_values(dbi, entry.id, status=StatusEnum.rejected)
    return [entry.db_id]


def rollback_scripts(dbi: DbInterface, entry: Any, script_type: ScriptType):
    for script in entry.scripts_:
        if script.script_type == script_type:
            Script.rollback_script(dbi, entry, script)


def rollback_workflows(dbi: DbInterface, entry: Any):
    for workflow in entry.w_:
        Workflow.rollback_script(dbi, entry, workflow)


def rollback_children(dbi: DbInterface, itr: Iterable, to_status: StatusEnum) -> list[DbId]:
    db_id_list: list[DbId] = []
    for entry in itr:
        if entry.status.value <= to_status.value:
            continue
        handler = entry.get_handler()
        db_id_list += handler.rollback(dbi, entry, to_status)
    return db_id_list


def rollback_entry(dbi: DbInterface, handler: Handler, entry: Any, to_status: StatusEnum) -> list[DbId]:
    status_val = entry.status.value
    db_id_list: list[DbId] = []
    if status_val <= to_status.value:
        return db_id_list
    while status_val >= to_status.value:
        if status_val == StatusEnum.completed.value:
            rollback_scripts(dbi, entry, ScriptType.validate)
        elif status_val == StatusEnum.collectable.value:
            rollback_scripts(dbi, entry, ScriptType.collect)
        elif status_val == StatusEnum.prepared.value:
            db_id_list += handler.rollback_run(dbi, entry, to_status)
        elif status_val == StatusEnum.ready.value:
            rollback_scripts(dbi, entry, ScriptType.prepare)
        db_id_list.append(entry.db_id)
        status_val -= 1
    entry.update_values(dbi, entry.id, status=to_status)
    return db_id_list
