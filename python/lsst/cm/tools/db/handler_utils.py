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
from lsst.cm.tools.db.job import Job
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
    StatusEnum.prepared: StatusEnum.running,
    StatusEnum.running: StatusEnum.running,
    StatusEnum.completed: StatusEnum.collectable,
}


def extract_child_status(itr: Iterable) -> np.ndarray:
    """Return the status of all children in an array"""
    return np.array([x.status.value for x in itr if not x.superseeded])


def extract_scripts_status(itr: Iterable, script_type: ScriptType) -> np.ndarray:
    """Return the status of all children in an array"""
    return np.array([x.status.value for x in itr if (x.script_type == script_type) and not x.superseeded])


def extract_job_status(itr: Iterable) -> np.ndarray:
    """Return the status of all children in an array"""
    return np.array([x.status.value for x in itr if not x.superseeded])


def check_children(entry: Any, min_status: StatusEnum, max_status: StatusEnum) -> StatusEnum:
    """Check the status of childern of a given entry
    and return a status accordingly

    Notes
    -----
    When an entry is waiting on children, it's status will only
    vary between a couple of states, but the status of the children
    can vary a lot more.

    The `min_status` and `max_status` parameters allow this
    function to return values that are consistent with the
    parent status.
    """
    child_status = extract_child_status(entry.children())
    if not child_status.size:  # pragma: no cover
        raise ValueError("Where have the children gone?")
    if (child_status >= StatusEnum.accepted.value).all():
        return StatusEnum.collectable
    status_val = min(max_status.value, max(min_status.value, child_status.min()))
    return StatusEnum(status_val)


def check_scripts(dbi: DbInterface, scripts: Iterable, script_type: ScriptType) -> StatusEnum:
    """Check the status all the scripts of a given type"""
    for script in scripts:
        if script.script_type != script_type:
            continue
        if script.superseeded:
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


def check_jobs(dbi: DbInterface, jobs: Iterable) -> StatusEnum:
    """Check the status of a set of jobs"""
    for job in jobs:
        if job.superseeded:
            continue
        Job.check_status(dbi, job)
    job_status = extract_job_status(jobs)
    assert job_status.size
    if (job_status >= StatusEnum.completed.value).all():
        return StatusEnum.completed
    if (job_status < 0).any():
        return StatusEnum.failed
    if (job_status >= StatusEnum.running.value).any():
        return StatusEnum.running
    if (job_status >= StatusEnum.prepared.value).any():
        return StatusEnum.prepared
    return StatusEnum.ready


def check_prepare_scripts(dbi: DbInterface, entry: Any) -> StatusEnum:
    """Check the status of the prepares scripts for one entry"""
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


def check_running_jobs(dbi: DbInterface, workflow: Workflow) -> StatusEnum:
    """Check the status of the jobs for one workflow"""
    workflow_status = check_jobs(dbi, workflow.jobs_)
    return workflow_status_map[workflow_status]


def check_entry(dbi: DbInterface, entry: Any) -> list[DbId]:
    """Check the status of a given entry, and take any
    possible actions to continue processing that entry

    This will continue checking this entry until:

    1. the entry is marked as rejected or failed,

    2. the entry is marked as accepted,

    3. the entry is marked as reviewable, which will
    require someone to actually review it,

    4. the entry is stays in the same status through
    and entire cycle, typically this is because it
    is waiting on some asynchronous event, such as jobs
    processing.
    """
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
        elif current_status == StatusEnum.prepared:
            handler.run(dbi, entry)
            new_status = StatusEnum.running
        elif current_status == StatusEnum.running:
            if entry.level == LevelEnum.workflow:
                new_status = check_running_jobs(dbi, entry)
            else:
                new_status = check_children(entry, StatusEnum.running, StatusEnum.collectable)
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
    """Check the status of a set of entries"""
    db_id_list = []
    for entry in itr:
        db_id_list += check_entry(dbi, entry)
    return db_id_list


def prepare_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    """Prepare an entry for processing

    This will take an entry from
    `StatusEnum.waiting` or `StatusEnum.ready`

    `StatusEnum.preparing` if the prepare scripts are run asynchronously
    `StatusEnum.prepared` if the prepare scripts completed
    """
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


def run_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    """Mark that an entry is processing

    This will take an entry from
    `StatusEnum.prepared` or `StatusEnum.running`

    This doesn't actually submit to the batch farm, as that
    needs to be throttled, but it does allow for jobs associated
    with the entry to be submitted.
    """
    return handler.run_hook(dbi, entry)


def run_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    """Call run_entry on all the children of given entry"""
    db_id_list: list[DbId] = []
    for child in children:
        if child.status != StatusEnum.prepared:
            continue
        handler = child.get_handler()
        db_id_list += run_entry(dbi, handler, child)
    return db_id_list


def collect_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    """Collect the data from the children of an entry

    This will take an entry from
    `StatusEnum.collectable` to

    `StatusEnum.collecting` if the collect scripts are run asynchronously
    `StatusEnum.completed` if the collect scripts are completed
    """
    assert entry.status == StatusEnum.collectable
    collect_scripts = handler.collect_script_hook(dbi, entry)
    if collect_scripts:
        status = StatusEnum.collecting
    else:
        status = StatusEnum.completed
    entry.update_values(dbi, entry.id, status=status)
    return [entry.db_id]


def collect_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    """Call collect_entry on all the children of given entry"""
    db_id_list: list[DbId] = []
    for child in children:
        if child.status != StatusEnum.collectable:
            continue
        handler = child.get_handler()
        db_id_list += collect_entry(dbi, handler, child)
    return db_id_list


def validate_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    """Run the validation scripts for an entry

    This will take an entry from
    `StatusEnum.completed` to

    `StatusEnum.validating` if the validation scripts are run asynchronously
    `StatusEnum.accepted` if the validation scripts are completed
    """

    assert entry.status == StatusEnum.completed
    validate_scripts = handler.validate_script_hook(dbi, entry)
    if validate_scripts:
        status = StatusEnum.validating
    else:
        status = StatusEnum.accepted
    entry.update_values(dbi, entry.id, status=status)
    return [entry.db_id]


def validate_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    """Call validate_entry on all the children of given entry"""
    db_id_list: list[DbId] = []
    for child in children:
        if child.status != StatusEnum.completed:
            continue
        handler = child.get_handler()
        db_id_list += validate_entry(dbi, handler, child)
    return db_id_list


def accept_scripts(dbi: DbInterface, scripts: Iterable) -> None:
    """Make all the scripts associated with an entry as accepted"""
    for script in scripts:
        assert script.status == StatusEnum.completed
        script.update_values(dbi, script.id, status=StatusEnum.accepted)


def accept_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    """Accept an entry that needed reviewing

    This will take an entry from
    `StatusEnum.reviewable` to `StatusEnum.accepted`
    """

    db_id_list: list[DbId] = []
    if entry.status != StatusEnum.reviewable:
        return db_id_list
    handler.accept_hook(dbi, entry)
    accept_scripts(dbi, entry.scripts_)
    db_id_list += [entry.db_id]
    entry.update_values(dbi, entry.id, status=StatusEnum.accepted)
    return db_id_list


def accept_children(dbi: DbInterface, children: Iterable) -> list[DbId]:
    """Call accept_entry on all the children of given entry"""
    db_id_list = []
    for child in children:
        handler = child.get_handler()
        db_id_list += handler.accept(dbi, child)
    return db_id_list


def reject_entry(dbi: DbInterface, handler: Handler, entry: Any) -> list[DbId]:
    """Reject an entry

    Notes
    -----
    This will block processing of any parents until this entry
    is superseeded.

    Trying to reject an already accepted entry will raise
    an exception.  This is because if the entry is accepted,
    the data can be used in processing up the hierarchy.

    Rejecting such a entry will require rolling back the
    parent.
    """
    if entry.status == StatusEnum.accepted:
        raise ValueError("Rejecting an already accepted entry {entry.db_id}")
    entry.update_values(dbi, entry.id, status=StatusEnum.rejected)
    handler.reject_hook(dbi, entry)
    return [entry.db_id]


def rollback_scripts(dbi: DbInterface, entry: Any, script_type: ScriptType) -> None:
    for script in entry.scripts_:
        if script.script_type == script_type:
            Script.rollback_script(dbi, entry, script)


def rollback_workflows(dbi: DbInterface, entry: Any) -> None:
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
    """Mark an entry as superseeded"""
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
