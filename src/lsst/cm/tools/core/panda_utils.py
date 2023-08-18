from typing import Any, TextIO

import idds.common.utils as idds_utils
import pandaclient.idds_api
import yaml

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.slurm_utils import SlurmChecker
from lsst.cm.tools.core.utils import StatusEnum


def print_errors_aggregate(stream: TextIO, errors_aggregate: dict[int, dict[str, Any]]) -> None:
    """Print an aggregated list of all encounter errors."""
    copy_dict = {}
    for key, val in errors_aggregate.items():
        if not isinstance(val, list):
            copy_dict[key] = val
            continue
        copy_list = []
        for vv in val:
            copy_list.append(vv)
            if not isinstance(vv, dict):
                continue
            if "error_type" not in vv:
                continue
            if vv["error_type"] is None:
                continue
            copy_list[-1]["error_type"] = vv["error_type"].id
        copy_dict[key] = copy_list
    yaml.dump(copy_dict, stream)


def parse_bps_stdout(url: str) -> dict[str, str]:
    """Parse the std from a bps submit job"""
    out_dict = {}
    with open(url, "r", encoding="utf8") as fin:
        line = fin.readline()
        while line:
            tokens = line.split(":")
            if len(tokens) != 2:  # pragma: no cover
                line = fin.readline()
                continue
            out_dict[tokens[0]] = tokens[1]
            line = fin.readline()
    return out_dict


# dict to map trans diagnostic codes to an associated useful message,
# placeholder until more is handled externally.
trans_diag_map = dict(
    t1="Pipelines error: check logging.",
    t129="SIGHUP: Hangup detected in controlling terminal or death of \
    controlling process.",
    t130="SIGINT: Interrupt from keyboard.",
    t131="SIGQUIT: Quit from keyboard.",
    t132="SIGILL: Illegal instruction.",
    t133="SIGTRAP: Trace/breakpoint trap.",
    t134="SIGABRT: Abort signal.",
    t135="SIGBUS: Bus error (bad memory access).",
    t136="SIGFPE: Floating Point Exemption.",
    t137="SIGKILL: Kill signal.",
    t138="SIGUSR1: User defined signal.",
    t139="SIGSEGV: Invalid memory reference.",
    t140="SIGUSR2: User defined signal.",
    t141="SIGPIPE: Broken pipe.",
    t142="SIGALRM: Timer signal.",
    t143="SIGTERM: Termination signal.",
    t144="SIGSTKFLT: Stack fault on coprocessor.",
    t145="SIGCHLD: Child stopped or terminated.",
    t146="SIGCONT: Continue if stopped.",
    t147="SIGSTOP: Stop process.",
    t148="SIGTSTP: Stop typed at terminal.",
    t149="SIGTTIN: Terminal input for background process.",
    t150="SIGTTOU: Terminal output for background process.",
    t151="SIGURG: Urgent condition on socket.",
    t152="SIGXCPU: CPU Time Limit Exceeded.",
    t153="SIGXFSZ: File size limit exceeded.",
    t154="SIGVTALRM: Virtual alarm clock.",
    t155="SIGPROF: Profiling timer expired.",
    t156="SIGWINCH: Window resize signal.",
    t157="SIGIO: I/O now possible",
    t158="SIGPWR: Power failure",
    t159="SIGSYS: Bad system call",
)


def get_errors_from_jeditaskid(dbi: DbInterface, conn, panda_reqid: int, jeditaskid: int):  # pragma: no cover
    """Return the errors associated with a jeditaskid as
    a dictionary for each job.

    Parameters
    ----------
    dbi: DbInterface
        Used to look up the error types

    conn: IddsApiInteface
        A connection to IDDS.

    panda_reqid: int
        A pandaID that is shared by jeditaskids in the
        same workflow.

    jeditaskid: int
        A jeditaskid, which will have some number of
        pandaIDs associated.

    Returns
    -------
    error_dicts: list[dict]
        A list of dictionaries containing everything
        we want to update the error instance db with
    """
    ret = conn.get_contents_output_ext(request_id=panda_reqid, workload_id=jeditaskid)
    print(f"Checking {jeditaskid}")
    conn_status = ret[0]
    if len(ret[1][1]) == 1:
        wmskey = list(ret[1][1].keys())[0]
        tasks = ret[1][1][wmskey]
    else:
        # temporary test
        print(f"failed on {jeditaskid}")
        error_dicts = []
        return error_dicts
    if conn_status != 0:
        raise ValueError(f"Connection to Panda Failed with status {conn_status}")

    error_dicts = []

    # acquire information for any failed jobs that did run.
    failed_jobs = [
        job for job in tasks if int(job["trans_exit_code"]) != 0 and job["trans_exit_code"] is not None
    ]
    if len(failed_jobs) == 0:
        return error_dicts
    else:
        for job in failed_jobs:
            error_dict = dict()
            if int(job["trans_exit_code"]) != 1:
                error_dict["panda_err_code"] = "trans, " + str(job["trans_exit_code"])
                try:
                    trans_diag = trans_diag_map["t" + str(job["trans_exit_code"])]
                except KeyError:
                    trans_diag = "Stack error: check logging and report!"
                error_dict["diagnostic_message"] = trans_diag
            # pilot error
            elif int(job["trans_exit_code"]) == 1:
                if job["pilot_error_code"] != 0:
                    error_dict["panda_err_code"] = "pilot, " + str(job["pilot_error_code"])
                    error_dict["diagnostic_message"] = job["pilot_error_diag"]
                elif job["brokerage_error_code"] != 0:
                    error_dict["panda_err_code"] = "brokerage, " + str(job["brokerage_error_code"])
                    error_dict["diagnostic_message"] = job["brokerage_error_diag"]
                elif job["ddm_error_code"] != 0:
                    error_dict["panda_err_code"] = "ddm, " + str(job["ddm_error_code"])
                    error_dict["diagnostic_message"] = job["ddm_error_diag"]
                elif job["exe_error_code"] != 0:
                    error_dict["panda_err_code"] = "exe, " + str(job["exe_error_code"])
                    error_dict["diagnostic_message"] = job["exe_error_diag"]
                elif job["job_dispatcher_error_code"] != 0:
                    error_dict["panda_err_code"] = "jobdispatcher, " + str(job["job_dispatcher_error_code"])
                    error_dict["diagnostic_message"] = job["job_dispatcher_error_diag"]
                elif job["sup_error_code"] != 0:
                    error_dict["panda_err_code"] = "sup, " + str(job["sup_error_code"])
                    error_dict["diagnostic_message"] = job["sup_error_diag"]
                elif job["task_buffer_error_code"] != 0:
                    error_dict["panda_err_code"] = "taskbuffer, " + str(job["task_buffer_error_code"])
                    error_dict["diagnostic_message"] = job["task_buffer_error_diag"]
                else:
                    error_dict["panda_err_code"] = "unknown"
                    error_dict["diagnostic_message"] = "check the logs"
            else:
                raise RuntimeError("Not sure what kinda error we got")
            jobname_words = [word for word in job["job_name"].split("_") if word.isdigit() is False]
            error_dict["pipetask"] = jobname_words[-2]
            error_dict["log_file_url"] = job["pilot_id"].split("|")[0]
            # TODO: currently not found in PanDA job object
            # providing nearest substitute, the
            # quantum graph
            error_dict["data_id"] = job["name"]
            error_dict["error_type"] = dbi.match_error_type(
                error_dict["panda_err_code"], error_dict["diagnostic_message"]
            )

            error_dicts.append(error_dict)

        return error_dicts


def determine_error_handling(dbi: DbInterface, errors_agg: dict, max_pct_failed: dict) -> str:
    """Given a dict of errors, decide what the
    appropriate behavior is for the step.

    Parameters
    ----------
    dbi: DbInterface
        a connection to the database interface

    errors_agg: dict
        a dict of dict for each jtid with a recorded
        error message

    max_pct_failed: dict
        a dict for each jtid with the percent of
        failed files

    Returns
    -------
    panda_status: str
        a panda status determined based on reported
        errors
    """
    # bad untested psuedo code
    decision_results = []
    for key in errors_agg.keys():
        # for a given error, try to make a match
        error_items = errors_agg[key]
        pct_failed = max_pct_failed[key]
        for error_item in error_items:
            try:
                error_match = dbi.match_error_type(
                    error_item["panda_err_code"], error_item["diagnostic_message"]
                )
            except NameError:
                error_match = False

            # if there is no match, mark it as reviewable
            if error_match in [False, None]:
                temp_status = "failed_review"
                # if this a known error critical enough that we need to pause
                # then pause.
            elif error_match.error_flavor is not None and error_match.error_flavor.name == "critical":
                temp_status = "failed_pause"
                # if it is not a payload error nor critical, start a rescue
            elif error_match.error_flavor is not None and error_match.error_flavor.name != "payload":
                temp_status = "failed_rescue"
                # if the payload error is marked as rescueable, rescue
            elif error_match.is_rescueable is True:
                temp_status = "failed_rescue"
                # is it supposed to be resolved?
            elif error_match.is_resolved is True:
                temp_status = "failed_review"
            else:
                # rework to count over the entire step and
                # sum over the errors
                max_intensity = error_match.max_intensity
                if pct_failed >= max_intensity:
                    temp_status = "failed_review"
                else:
                    temp_status = "done"
            decision_results.append(temp_status)

    # now based on the worst result in decison_results, set panda_status
    if "failed_pause" in decision_results:
        panda_status = "failed_pause"
    elif "failed_review" in decision_results:
        panda_status = "failed_review"
    elif "failed_rescue" in decision_results:
        panda_status = "failed_rescue"
    else:
        panda_status = "done"

    return panda_status


# map to take the many statuses and map them to end results
jtid_status_map = dict(
    New="running",
    Ready="running",
    Transforming="running",
    Finished="done",
    SubFinished="finished",
    Failed="failed",
    Extend="running",
    ToCancel="failed",
    Cancelling="failed",
    Cancelled="failed",
    ToSuspend="running",
    Suspending="running",
    Suspended="running",
    ToResume="running",
    Resuming="running",
    ToExpire="failed",
    Expiring="failed",
    Expired="failed",
    ToFinish="running",
    ToForceFinish="failed",
)


def decide_panda_status(dbi: DbInterface, statuses: list, errors_agg: dict, max_pct_failed: dict) -> str:
    """Look at the list of statuses for each
    jeditaskid and return a choice for the entire
    reqid status

    Parameters
    ----------
    dbi: DbInterface
        a connection to the database interface

    statuses: list
        a list of statuses for each jeditaskid
        in the reqid

    errors_agg: dict
        a dict of dicts for each jtid with recorded
        error messages

    max_pct_failed: dict
        a dict for each jtid with the percent
        of failed files

    Returns
    -------
    panda_status: str
        the panda job status
    """
    # take our statuses and convert them
    status_mapped = [jtid_status_map[status] for status in statuses]

    if "running" in status_mapped:
        panda_status = "running"
    elif "failed" in status_mapped:
        panda_status = "failed"
    elif "finished" in status_mapped:
        # if the task returns as finished,
        # take errors -> return status
        panda_status = determine_error_handling(dbi, errors_agg, max_pct_failed)
    elif "done" in status_mapped:
        panda_status = "done"
    elif not status_mapped:
        panda_status = "running"
    else:  # pragma: no cover
        raise ValueError(
            f"decide_panda_status failed to make a decision based on this status vector: {str(status_mapped)}"
        )
    return panda_status


def check_panda_status(
    dbi: DbInterface, panda_reqid: int, panda_username=None, while_running=False
) -> str:  # pragma: no cover
    """Check the errors for a given panda reqid and
    return a final overarching error

    Parameters
    ----------
    dbi: DbInterface
        a connection to the database interface
    panda_reqid: int
        a reqid associated with the job
    panda_username: str
        None by default, username required for other
        submissions
    while_running: Bool
        False by default, allows for limited checking
        of jobs still running

    Returns
    -------
    panda_status: str
        the panda job status

    error_aggregate: list[dict]
        A list of dictionaries containing everything
        we want to update the error instance db with
    """
    # first pull down all the tasks
    errors_aggregate, tasks, merging = get_panda_errors(dbi, int(panda_reqid), panda_username, while_running)
    if not merging:
        return "running", {}

    statuses = [task["transform_status"]["attributes"]["_name_"] for task in tasks]
    # then pull all the errors for the tasks
    max_pct_failed = dict()
    jtids = [
        task["transform_workload_id"]
        for task in tasks
        if task["transform_status"]["attributes"]["_name_"] != "Finished"
    ]
    pct_files_failed = [
        task["output_failed_files"] / (task["output_failed_files"] + task["output_processed_files"])
        for task in tasks
        if task["transform_status"]["attributes"]["_name_"] != "Finished"
    ]
    # need to make a matching dict form
    for jtid, pctfailed in zip(jtids, pct_files_failed):
        max_pct_failed[jtid] = pctfailed

    # now determine a final answer based on statuses for the entire reqid
    panda_status = decide_panda_status(dbi, statuses, errors_aggregate, max_pct_failed)

    return panda_status, errors_aggregate


def get_panda_errors(
    dbi: DbInterface, panda_reqid: int, panda_username=None, while_running=False
) -> tuple[Any]:  # pragma: no cover
    """Get panda errors for a given reqID."""
    conn = pandaclient.idds_api.get_api(idds_utils.json_dumps, idds_host=None, compress=True, manager=True)
    ret = conn.get_requests(request_id=int(panda_reqid), with_detail=True)
    errors_aggregate = dict()
    has_merging = False
    tasks = ret[1][1]
    for task in tasks:
        if task["transform_name"].find("finalJob") >= 0 or task["transform_name"].find("xecutionButler") >= 0:
            has_merging = True
    if not has_merging and not while_running:
        return {}, tasks, False

    if while_running:
        print("Checking for errors.")
    jtids = [
        task["transform_workload_id"]
        for task in tasks
        if task["transform_status"]["attributes"]["_name_"] != "Finished"
    ]
    for jtid in jtids:
        errors_dict = get_errors_from_jeditaskid(dbi, conn, int(panda_reqid), jtid)
        errors_aggregate[jtid] = errors_dict
    if while_running:
        for jtid in jtids:
            for error_dict in errors_aggregate[jtid]:
                print(
                    f'JTID: {jtid}, Error: {error_dict["panda_err_code"]}, \
                Diag: {error_dict["diagnostic_message"]}'
                )
        return {}, tasks, False
    return errors_aggregate, tasks, True


class PandaChecker(SlurmChecker):  # pragma: no cover
    """Checker to use a slurm job_id and panda_id
    to check job status
    """

    status_map = dict(
        done=StatusEnum.completed,
        failed=StatusEnum.failed,
        finished=StatusEnum.failed,
        pending=StatusEnum.running,
        registered=StatusEnum.running,
        running=StatusEnum.running,
    )

    panda_status_map = dict(
        done=StatusEnum.completed,
        running=StatusEnum.running,
        accept=StatusEnum.completed,
        failed=StatusEnum.reviewable,
        failed_rescue=StatusEnum.rescuable,
        failed_review=StatusEnum.reviewable,
        # TODO: add handling for cleanup state and an associated Enum
        failed_cleanup=StatusEnum.reviewable,
        # TODO: add handling for a pause state and an associated Enum
        failed_pause=StatusEnum.reviewable,
    )

    def check_url(self, dbi: DbInterface, job: JobBase) -> dict[str, Any]:
        update_vals: dict[str, Any] = {}
        if job.status not in [
            StatusEnum.populating,
            StatusEnum.running,
        ]:
            return update_vals
        panda_url = job.panda_url
        if panda_url is None:
            slurm_dict = SlurmChecker.check_url(self, dbi, job)
            if not slurm_dict:
                return update_vals
            batch_status = slurm_dict.get("batch_status", job.batch_status)
            if batch_status != job.batch_status:
                update_vals["batch_status"] = batch_status
            if slurm_dict.get("status") == StatusEnum.completed:
                bps_dict = parse_bps_stdout(job.log_url)
                panda_url = bps_dict["Run Id"]
                update_vals["panda_url"] = panda_url.strip()
            elif slurm_dict.get("status") == StatusEnum.failed:
                update_vals["status"] = StatusEnum.failed
                return update_vals
        if panda_url is None:
            return update_vals
        panda_status, errors_aggregate = check_panda_status(
            dbi, int(panda_url), self.generic_username, self.while_running
        )
        if panda_status != job.panda_status:
            update_vals["panda_status"] = panda_status

        dbi.commit_errors(job.id, errors_aggregate)
        status = self.panda_status_map[panda_status]
        if status != job.status:
            update_vals["status"] = status
        return update_vals

    @classmethod
    def check_panda_status(cls, dbi: DbInterface, panda_reqid: int, panda_username=None) -> str:
        return check_panda_status(dbi, panda_reqid, panda_username)

    @classmethod
    def get_panda_errors(
        cls, dbi: DbInterface, panda_reqid: int, panda_username=None
    ) -> dict[int, dict[str, Any]]:
        return get_panda_errors(dbi, panda_reqid, panda_username)
