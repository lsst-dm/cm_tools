from typing import Any, Iterable

import numpy as np

from lsst.cm.tools.core.db_interface import DbInterface
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, ScriptType, StatusEnum
from lsst.cm.tools.db.job import Job
from lsst.cm.tools.db.script import Script

# These functions map one level of StatusEnum
# to another: e.g. prepare_script_status_map
# reading as accepted or completed means that
# the part of the code that prepares the scripts is
# done and the job can be marked as prepared
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


def extract_child_status(itr: Iterable, min_status: StatusEnum, max_status: StatusEnum) -> StatusEnum:
    """Return the status of all children in an array"""
    child_status = np.array([x.status.value for x in itr if not x.superseded])
    if not child_status.size:  # pragma: no cover
        return min_status
    if (child_status >= StatusEnum.accepted.value).all():
        return max_status
    status_val = min(max_status.value, max(min_status.value, child_status.min()))
    return StatusEnum(status_val)


def extract_completion_status(itr: Iterable, min_status: StatusEnum, max_status: StatusEnum) -> StatusEnum:
    """Return the status of all children in an array,
    specific to running jobs to collectable"""
    child_status = np.array([x.status.value for x in itr if not x.superseded])
    if not child_status.size:  # pragma: no cover
        return min_status
    if (child_status >= StatusEnum.accepted.value).all() and (
        child_status == StatusEnum.accepted.value
    ).any():
        return max_status
    return min_status


def extract_scripts_status(itr: Iterable, script_type: ScriptType) -> StatusEnum:
    """Check the status all the scripts of a given type"""
    scripts_status = np.array(
        [x.status.value for x in itr if (x.script_type == script_type) and not x.superseded]
    )
    # should never be called on entries with no scripts
    assert scripts_status.size
    if (scripts_status >= StatusEnum.accepted.value).all():
        return StatusEnum.accepted
    if (scripts_status >= StatusEnum.completed.value).all():
        return StatusEnum.completed
    if (scripts_status < 0).any():
        return StatusEnum.failed
    if (scripts_status >= StatusEnum.running.value).any():
        return StatusEnum.running
    return StatusEnum.ready


def extract_job_status(itr: Iterable) -> StatusEnum:
    """Check the status of a set of jobs"""
    job_status = np.array([x.status.value for x in itr if not x.superseded])
    assert job_status.size

    if (job_status >= StatusEnum.rescuable.value).all():
        return StatusEnum.rescuable
    if (job_status >= StatusEnum.accepted.value).all():
        return StatusEnum.accepted
    if (job_status >= StatusEnum.reviewable.value).all():
        return StatusEnum.reviewable
    if (job_status >= StatusEnum.completed.value).all():
        return StatusEnum.completed
    if (job_status < 0).any():
        return StatusEnum.failed
    if (job_status >= StatusEnum.running.value).any():
        return StatusEnum.running
    if (job_status >= StatusEnum.prepared.value).any():
        return StatusEnum.prepared
    return StatusEnum.ready


def check_scripts(dbi: DbInterface, entry: Any, script_type: ScriptType) -> None:
    """Check the status all the scripts of a given type"""
    for script in entry.all_scripts_:
        if script.script_type != script_type:
            continue
        if script.superseded:
            continue
        Script.check_status(dbi, script)
    dbi.connection().commit()


def check_jobs(dbi: DbInterface, entry: Any) -> None:
    """Check the status of a set of jobs"""
    for job in entry.jobs_:
        if job.superseded:
            continue
        Job.check_status(dbi, job)
    dbi.connection().commit()


def check_waiting_entry(dbi: DbInterface, entry: Any) -> bool:
    if entry.check_prerequistes(dbi):
        handler = entry.get_handler()
        handler.make_scripts(dbi, entry)
        new_status = StatusEnum.ready
        entry.update_values(dbi, entry.id, status=new_status)
        return True
    return False


def check_ready_entry(dbi: DbInterface, entry: Any) -> bool:
    handler = entry.get_handler()
    new_status = handler.prepare(dbi, entry)
    entry.update_values(dbi, entry.id, status=new_status)
    return True


def check_preparing_entry(dbi: DbInterface, entry: Any) -> bool:
    current_status = entry.status
    script_status = extract_scripts_status(entry.scripts_, ScriptType.prepare)
    new_status = prepare_script_status_map[script_status]
    if current_status != new_status:
        entry.update_values(dbi, entry.id, status=new_status)
        return True
    return False


def check_prepared_entry(dbi: DbInterface, entry: Any) -> bool:
    handler = entry.get_handler()
    new_status = handler.make_children(dbi, entry)
    entry.update_values(dbi, entry.id, status=new_status)
    return True


def check_populating_entry(dbi: DbInterface, entry: Any) -> bool:
    current_status = entry.status
    new_status = extract_child_status(entry.children(), StatusEnum.populating, StatusEnum.running)
    if current_status != new_status:
        entry.update_values(dbi, entry.id, status=new_status)
        return True
    return False


def check_running_entry(dbi: DbInterface, entry: Any) -> bool:
    current_status = entry.status
    if entry.level == LevelEnum.workflow:
        new_status = extract_job_status(entry.jobs_)
    else:
        new_status = extract_completion_status(entry.children(), StatusEnum.running, StatusEnum.collectable)
    if current_status != new_status:
        entry.update_values(dbi, entry.id, status=new_status)
        return True
    return False


def check_collectable_entry(dbi: DbInterface, entry: Any) -> bool:
    handler = entry.get_handler()
    new_status = handler.collect(dbi, entry)
    entry.update_values(dbi, entry.id, status=new_status)
    return True


def check_collecting_entry(dbi: DbInterface, entry: Any) -> bool:
    current_status = entry.status
    script_status = extract_scripts_status(entry.scripts_, ScriptType.collect)
    new_status = collect_script_status_map[script_status]
    if current_status != new_status:
        entry.update_values(dbi, entry.id, status=new_status)
        return True
    return False


def check_completed_entry(dbi: DbInterface, entry: Any) -> bool:
    handler = entry.get_handler()
    new_status = handler.validate(dbi, entry)
    entry.update_values(dbi, entry.id, status=new_status)
    return True


def check_validating_entry(dbi: DbInterface, entry: Any) -> bool:
    current_status = entry.status
    script_status = extract_scripts_status(entry.scripts_, ScriptType.validate)
    new_status = validate_script_status_map[script_status]
    if current_status != new_status:
        entry.update_values(dbi, entry.id, status=new_status)
        return True
    return False


def do_entry_loop(dbi: DbInterface, entry: Any, status: StatusEnum, func: Any) -> bool:
    has_updates = False
    # print(f"do_entry_loop {status.name} {str(func)} {entry.level.name}")
    level_counter = {}
    if entry.status == status:
        level_counter[entry.level.name] = 1
        has_updates |= func(dbi, entry)
    sub_level = LevelEnum.workflow
    while sub_level != entry.level:
        counter = 0
        matching = dbi.get_matching(sub_level, entry, status)
        for entry_ in matching:
            assert entry_[0].status == status
            has_updates |= func(dbi, entry_[0])
            counter += 1
        if counter:
            level_counter[sub_level.name] = counter
        sub_level = sub_level.parent()
    # print(f"  checked {str(level_counter)}: {has_updates}")
    if has_updates:
        dbi.connection().commit()
    return has_updates


def check_entry_loop_iteration(dbi: DbInterface, entry: Any) -> bool:
    can_continue = False

    can_continue = do_entry_loop(dbi, entry, StatusEnum.waiting, check_waiting_entry)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.ready, check_ready_entry)
    check_scripts(dbi, entry, ScriptType.prepare)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.preparing, check_preparing_entry)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.prepared, check_prepared_entry)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.populating, check_populating_entry)
    check_jobs(dbi, entry)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.running, check_running_entry)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.collectable, check_collectable_entry)
    check_scripts(dbi, entry, ScriptType.collect)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.collecting, check_collecting_entry)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.completed, check_completed_entry)
    check_scripts(dbi, entry, ScriptType.validate)
    can_continue |= do_entry_loop(dbi, entry, StatusEnum.validating, check_validating_entry)

    if can_continue:
        dbi.connection().commit()

    return can_continue


def check_entry_loop(dbi: DbInterface, entry: Any) -> bool:
    can_continue = True
    while can_continue:
        can_continue = check_entry_loop_iteration(dbi, entry)
    return entry.status


def accept_jobs(dbi: DbInterface, jobs: Iterable, rescuable: bool = False) -> None:
    """Make all the scripts associated with an entry as accepted"""
    for job in jobs:
        if job.superseded:
            continue
        if job.status != StatusEnum.reviewable:
            continue
        if rescuable:
            job.update_values(dbi, job.id, status=StatusEnum.rescuable)
        else:
            job.update_values(dbi, job.id, status=StatusEnum.accepted)


def accept_scripts(dbi: DbInterface, scripts: Iterable) -> None:
    """Make all the scripts associated with an entry as accepted"""
    for script in scripts:
        # accept_scripts should only be called on completed scripts
        # assert script.status == StatusEnum.completed
        script.update_values(dbi, script.id, status=StatusEnum.accepted)


def accept_entry(dbi: DbInterface, handler: Handler, entry: Any, rescuable: bool = False) -> list[DbId]:
    """Accept an entry that needed reviewing

    This will take an entry from
    `StatusEnum.reviewable` to `StatusEnum.accepted`
    """
    db_id_list: list[DbId] = []
    if entry.status == StatusEnum.running:
        check_entry_loop_iteration(dbi, entry)
    if entry.status != StatusEnum.reviewable:
        return db_id_list
    handler.accept_hook(dbi, entry)
    accept_scripts(dbi, entry.scripts_)
    db_id_list += [entry.db_id]
    if entry.level == LevelEnum.workflow:
        accept_jobs(dbi, entry.jobs_, rescuable)
    if rescuable:
        entry.update_values(dbi, entry.id, status=StatusEnum.rescuable)
    else:
        entry.update_values(dbi, entry.id, status=StatusEnum.accepted)
    return db_id_list


def accept_children(dbi: DbInterface, children: Iterable, rescuable: bool = False) -> list[DbId]:
    """Call accept_entry on all the children of given entry"""
    db_id_list = []
    for child in children:
        handler = child.get_handler()
        db_id_list += accept_entry(dbi, handler, child, rescuable)
    return db_id_list


def reject_entry(dbi: DbInterface, handler: Handler, entry: Any, purge: bool = False) -> list[DbId]:
    """Reject an entry

    Notes
    -----
    This will block processing of any parents until this entry
    is superseded.

    Trying to reject an already accepted entry will raise
    an exception.  This is because if the entry is accepted,
    the data can be used in processing up the hierarchy.

    Rejecting such a entry will require rolling back the
    parent.
    """
    if entry.status == StatusEnum.accepted:
        raise ValueError("Rejecting an already accepted entry {entry.db_id}")
    entry.update_values(dbi, entry.id, status=StatusEnum.rejected)
    handler.reject_hook(dbi, entry, purge)
    return [entry.db_id]


def rollback_scripts(dbi: DbInterface, entry: Any, script_type: ScriptType, purge: bool = False) -> None:
    """Rollback scripts associated with an entry"""
    for script in entry.scripts_:
        if script.script_type == script_type:
            Script.rollback_script(dbi, entry, script, purge)


def rollback_jobs(dbi: DbInterface, entry: Any, purge: bool = False) -> None:
    """Rollback jobs associated with an entry"""
    for job in entry.jobs_:
        Job.rollback_script(dbi, entry, job, purge)


def rollback_children(
    dbi: DbInterface, itr: Iterable, to_status: StatusEnum, purge: bool = False
) -> list[DbId]:
    """Rollback all members of an entry collection"""
    db_id_list: list[DbId] = []
    for entry in itr:
        if entry.status.value <= to_status.value:
            continue
        handler = entry.get_handler()
        db_id_list += handler.rollback(dbi, entry, to_status, purge)
    return db_id_list


def rollback_entry(
    dbi: DbInterface, handler: Handler, entry: Any, to_status: StatusEnum, purge: bool = False
) -> list[DbId]:
    """Roll-back an entry to a lower status"""
    status_val = entry.status.value
    if status_val < 0:
        status_val = StatusEnum.completed.value
    db_id_list: list[DbId] = []
    if status_val <= to_status.value:
        return db_id_list
    while status_val >= to_status.value:
        if status_val == StatusEnum.completed.value:
            rollback_scripts(dbi, entry, ScriptType.validate, purge)
        elif status_val == StatusEnum.collectable.value:
            rollback_scripts(dbi, entry, ScriptType.collect, purge)
        elif status_val == StatusEnum.populating.value:
            rollback_jobs(dbi, entry, purge)
            db_id_list += handler.rollback_subs(dbi, entry, StatusEnum.prepared, purge)
        elif status_val == StatusEnum.prepared.value:
            supersede_children(dbi, entry.children(), purge)
        elif status_val == StatusEnum.ready.value:
            rollback_scripts(dbi, entry, ScriptType.prepare, purge)
        status_val -= 1
    db_id_list.append(entry.db_id)

    entry.update_values(dbi, entry.id, status=to_status)
    return db_id_list


def supersede_children(dbi: DbInterface, itr: Iterable, purge: bool = False) -> list[DbId]:
    """Supersede all members of and entry collection"""
    db_id_list: list[DbId] = []
    for entry in itr:
        handler = entry.get_handler()
        db_id_list += handler.supersede(dbi, entry, purge)
    return db_id_list


def supersede_entry(dbi: DbInterface, handler: Handler, entry: Any, purge: bool = False) -> list[DbId]:
    """Supersede an entry"""
    entry.update_values(dbi, entry.id, superseded=True)
    handler.supersede_hook(dbi, entry, purge)
    return [entry.db_id]
