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
    StatusEnum.completed: StatusEnum.ready,
    StatusEnum.accepted: StatusEnum.ready,
}

collect_script_status_map = {
    StatusEnum.failed: StatusEnum.failed,
    StatusEnum.ready: StatusEnum.collecting,
    StatusEnum.running: StatusEnum.collecting,
    StatusEnum.completed: StatusEnum.completed,
    StatusEnum.accepted: StatusEnum.accepted,
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
    StatusEnum.completed: StatusEnum.collecting,
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


def check_children(entry: Any, itr: Iterable) -> StatusEnum:
    """Check the status of childern of a given row
    and return a status accordingly"""
    child_status = extract_child_status(itr)
    if child_status.size and (child_status >= StatusEnum.accepted.value).all():
        return StatusEnum.collectable
    return StatusEnum(max(entry.status.value, child_status.min()))


def collect_children(dbi: DbInterface, entry: Any) -> Script:
    """Make the script to collect output from children"""
    handler = entry.get_handler()
    return handler.collect_script_hook(dbi, entry.scripts_, entry)


def check_scripts(dbi: DbInterface, scripts: Iterable, script_type: ScriptType) -> StatusEnum:
    """Check the status all the scripts of a given type"""
    status_list = []
    for script in scripts:
        if script.script_type != script_type:
            continue
        status_list.append(Script.check_status(dbi, script))
    scripts_status = np.array([status_.value for status_ in status_list])
    scripts_status_check = extract_scripts_status(scripts, script_type)
    assert (scripts_status == scripts_status_check).all()
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
    status_list = [Workflow.check_status(dbi, workflow) for workflow in workflows]
    workflow_status = np.array([status_.value for status_ in status_list])
    workflow_status_check = extract_workflow_status(workflows)
    assert (workflow_status == workflow_status_check).all()
    if not workflow_status.size:
        # No scripts to check, return completed
        return StatusEnum.completed
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


def prepare_entry(dbi: DbInterface, handler: Handler, entry: Any) -> dict[str, StatusEnum]:
    full_path = os.path.join(entry.prod_base_url, entry.fullname)
    safe_makedirs(full_path)
    prepare_scripts = handler.prepare_script_hook(dbi, entry)
    if prepare_scripts:
        status = StatusEnum.preparing
    else:
        status = StatusEnum.ready
    return dict(status=status)


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
                handler.prepare(dbi, entry)
                new_status = StatusEnum.preparing
        elif current_status == StatusEnum.preparing:
            new_status = check_prepare_scripts(dbi, entry)
        elif current_status in [StatusEnum.ready, StatusEnum.pending, StatusEnum.running]:
            if entry.level == LevelEnum.group:
                new_status = check_workflows_for_entry(dbi, entry)
            elif entry.level == LevelEnum.step:
                new_status = check_children(entry, entry.g_)
            elif entry.level == LevelEnum.campaign:
                new_status = check_children(entry, entry.s_)
        elif current_status == StatusEnum.collectable:
            collect_children(dbi, entry)
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
    return db_id_list


def check_entries(dbi: DbInterface, itr: Iterable) -> list[DbId]:
    db_id_list = []
    for entry in itr:
        db_id_list += check_entry(dbi, entry)
    return db_id_list


def validate_scripts(dbi: DbInterface, scripts: Iterable) -> list[DbId]:
    for script in scripts:
        if script.script_type != ScriptType.validate:
            continue
        if script.superseeded:
            continue
        if script.status.bad():
            continue
        script.check_status(dbi)
    scripts_status = extract_scripts_status(scripts, ScriptType.validate)
    if not scripts_status.size:
        # No scripts to check, return completed
        return StatusEnum.accepted
    if (scripts_status >= StatusEnum.accepted.value).all():
        return StatusEnum.accepted
    return StatusEnum[min(scripts_status)]


def validate_entry(dbi: DbInterface, entry: Any) -> list[DbId]:
    db_id_list: list[DbId] = []
    if entry.status != StatusEnum.completed:
        return db_id_list
    orig_status = entry.status
    status = validate_scripts(dbi, entry.scripts_)
    new_status = validate_script_status_map[status]
    if new_status != orig_status:
        db_id_list.append(entry.db_id)
        entry.update_values(dbi, entry.id, status=new_status)
    return db_id_list


def validate_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    db_id_list: list[DbId] = []
    for child in children:
        db_id_list += validate_entry(dbi, child)
    return db_id_list


def accept_scripts(dbi: DbInterface, scripts: Iterable) -> None:
    for script in scripts:
        if script.status != StatusEnum.completed:
            continue
        script.udpate_values(dbi, script.id, status=StatusEnum.accepted)


def accept_entry(dbi: DbInterface, entry: Any) -> list[DbId]:
    db_id_list: list[DbId] = []
    if entry.status != StatusEnum.reviewable:
        return db_id_list
    accept_scripts(dbi, entry.scripts_)
    db_id_list += entry.db_id
    entry.update_values(dbi, entry.id, status=StatusEnum.accepted)
    return db_id_list


def accept_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    db_id_list = []
    for child in children:
        db_id_list += accept_entry(dbi, child)
    return db_id_list


def reject_entry(dbi: DbInterface, entry: Any) -> list[DbId]:
    if entry.status == StatusEnum.accepted:
        return []
    entry.update_values(dbi, entry.id, status=StatusEnum.rejected)
    return [entry.db_id]
