from typing import Any

from pandaclient import Client, panda_api

from lsst.cm.tools.core.db_interface import DbInterface, JobBase
from lsst.cm.tools.core.slurm_utils import SlurmChecker
from lsst.cm.tools.core.utils import StatusEnum


def parse_bps_stdout(url: str) -> dict[str, str]:
    """Parse the std from a bps submit job"""
    out_dict = {}
    with open(url, "r", encoding="utf8") as fin:
        line = fin.readline()
        while line:
            tokens = line.split(":")
            if len(tokens) != 2:
                line = fin.readline()
                continue
            out_dict[tokens[0]] = tokens[1]
            line = fin.readline()
    return out_dict


def get_jeditaskid_from_reqid(reqid: int, username: str) -> list[int]:
    """Return the jeditaskids associated with a reqid.

    Parameters
    ----------
    reqid: int
        PanDA reqid as reported to bps submit
    username: str
        Username of original submitter

    Returns
    -------
    jeditaskids: list[int]
        A list of all jeditaskIDs associated with the
        submitted reqid
    """
    # TODO: try to find a way to do this with Client to avoid the
    # requirement on username storage
    conn = panda_api.get_api()
    reqid_pull = conn.get_tasks(int(reqid), username=username)
    jeditaskids = [reqid["jeditaskid"] for reqid in reqid_pull]

    return jeditaskids


def get_errors_from_jeditaskid(dbi: DbInterface, jeditaskid: int):
    """Return the errors associated with a jeditaskid as
    a dictionary for each job.

    Parameters
    ----------

    dbi: DbInterface
        Used to look up the error types

    jeditaskid: int
        A jeditaskid, which will have some number of
        pandaIDs associated.

    Returns
    -------
    error_dicts: list[dict]
        A list of dictionaries containing everything
        we want to update the error instance db with
    """
    conn_status, task_status = Client.getJediTaskDetails({"jediTaskID": jeditaskid}, True, True)
    print(f"Checking {jeditaskid}")

    # grab all the PanDA IDs
    if conn_status == 0:
        job_ids = list(task_status["PandaID"])
        jobs_list = []
        if len(job_ids) > 1:
            chunksize = 2000  # max number of allowed connections to PanDA
            chunks = [job_ids[i : i + chunksize] for i in range(0, len(job_ids), chunksize)]
            for chunk in chunks:
                conn_status, ret_jobs = Client.getFullJobStatus(ids=chunk, verbose=False)
                if conn_status == 0:
                    jobs_list.extend(ret_jobs)
        elif len(job_ids) == 1:
            conn_status, ret_jobs = Client.getFullJobStatus(ids=job_ids, verbose=False)
            if conn_status == 0:
                jobs_list = ret_jobs
        else:
            return []
            # TODO: properly address this break condition,
            # because something went wrong
    else:
        raise ValueError(f"Connection to Panda Failed with status {conn_status}")

    # now we need to parse all the error codes for failed PandaIDs
    error_dicts = []

    failed_jobs = [job for job in jobs_list if job.jobStatus == "failed"]
    if len(failed_jobs) == 0:
        return error_dicts
    else:
        for job in failed_jobs:
            error_dict = dict()
            # TODO: store the hecking pandaIDs so people can look things up

            # brokerageErrorCode/Diag
            if job.brokerageErrorCode != 0:
                error_dict["panda_err_code"] = "brokerage, " + str(job.brokerageErrorCode)
                error_dict["diagnostic_message"] = job.brokerageErrorDiag
            # ddmErrorCode/Diag
            elif job.ddmErrorCode != 0:
                error_dict["panda_err_code"] = "ddm, " + str(job.ddmErrorCode)
                error_dict["diagnostic_message"] = job.ddmErrorDiag
            # exeErrorCode/Diag
            elif job.exeErrorCode != 0:
                error_dict["panda_err_code"] = "exe, " + str(job.exeErrorCode)
                error_dict["diagnostic_message"] = job.exeErrorDiag
            # jobDispatcherErrorCode/Diag
            elif job.jobDispatcherErrorCode != 0:
                error_dict["panda_err_code"] = "jobDispatcher, " + str(job.jobDispatcherErrorCode)
                error_dict["diagnostic_message"] = job.jobDispatcherErrorDiag
            # pilotErrorCode/Diag
            elif job.pilotErrorCode != 0:
                error_dict["panda_err_code"] = "pilot, " + str(job.pilotErrorCode)
                error_dict["diagnostic_message"] = job.pilotErrorDiag
            # supErrorCode/Diag
            elif job.supErrorCode != 0:
                error_dict["panda_err_code"] = "sup, " + str(job.supErrorCode)
                error_dict["diagnostic_message"] = job.supErrorDiag
            # taskBufferErrorCode/Diag
            elif job.taskBufferErrorCode != 0:
                error_dict["panda_err_code"] = "taskBuffer, " + str(job.taskBufferErrorCode)
                error_dict["diagnostic_message"] = job.taskBufferErrorDiag
            # transExitCode (no Diag)
            elif job.transExitCode != 0:
                error_dict["panda_err_code"] = "trans, " + str(job.transExitCode)
                error_dict["diagnostic_message"] = "check the logging"
            else:
                raise RuntimeError("Not sure what kinda error we got")
            error_dict["function"] = job.jobName.split("_")[-3]
            error_dict["log_file_url"] = job.pilotID.split("|")[0]
            # TODO: currently not found in PanDA job object
            # providing nearest substitute, the
            # quantum graph
            error_dict["data_id"] = (job.Files[0]).lfn
            error_dict["error_type"] = dbi.match_error_type(
                error_dict["panda_err_code"], error_dict["diagnostic_message"]
            )

            error_dicts.append(error_dict)

        # TODO: code to update the ErrorInstance db with this
        # information

        return error_dicts


def decide_panda_status(statuses: list, errors_agg: dict) -> str:
    """Look at the list of statuses for each
    jeditaskid and return a choice for the entire
    reqid status

    Parameters
    ----------
    statuses: list
        a list of statuses for each jeditaskid
        in the reqid

    errors_agg: dict
        a dict of dicts for each jtid with recorded
        error messages

    Returns
    -------
    panda_status: str
        the panda job status
    """
    # map to take the many statuses and map them to end results
    jtid_status_map = dict(
        topreprocess="running",
        registered="running",
        tobroken="failed",
        broken="failed",
        preprocessing="running",
        defined="running",
        pending="running",
        ready="running",
        assigning="running",
        paused="running",
        aborting="failed",
        aborted="failed",
        running="running",
        throttled="running",
        scouting="running",
        scouted="running",
        finishing="running",
        passed="running",
        exhausted="failed",
        finished="failed",
        done="done",
        toretry="running",
        failed="failed",
        toincexec="running",
    )
    # take our statuses and convert them
    status_mapped = [jtid_status_map[status] for status in statuses]

    if "running" in status_mapped:
        panda_status = "running"
    elif "failed" in status_mapped:
        panda_status = "failed"
    # TODO: nuance case where finished can get
    # moved to done
    elif "done" in status_mapped:
        panda_status = "done"
    elif not status_mapped:
        panda_status = "running"
    else:
        raise ValueError(
            f"decide_panda_status failed to make a decision based on this status vector: {str(status_mapped)}"
        )
    return panda_status


def check_panda_status(dbi: DbInterface, panda_reqid: int, panda_username=None) -> str:
    """Check the errors for a given panda reqid and
    return a final overarching error

    Parameters
    ----------
    panda_reqid: int
        a reqid associated with the job
    panda_username: str
        None by default, username required for other
        submissions

    Returns
    -------
    panda_status: str
        the panda job status

    error_aggregate: list[dict]
        A list of dictionaries containing everything
        we want to update the error instance db with
    """

    # first pull down all the tasks
    conn = panda_api.get_api()
    tasks = conn.get_tasks(int(panda_reqid), username=panda_username)
    statuses = [task["status"] for task in tasks]

    print(statuses)

    # then pull all the errors for the tasks
    errors_aggregate = dict()
    jtids = [task["jeditaskid"] for task in tasks if task["status"] != "done"]
    for jtid in jtids:
        errors_aggregate[str(jtid)] = get_errors_from_jeditaskid(dbi, jtid)

    # now determine a final answer based on statuses for the entire reqid
    panda_status = decide_panda_status(statuses, errors_aggregate)

    return panda_status, errors_aggregate


def get_panda_errors(dbi: DbInterface, panda_reqid: int, panda_username=None) -> dict[int, dict[str, Any]]:
    conn = panda_api.get_api()
    tasks = conn.get_tasks(int(panda_reqid), username=panda_username)
    errors_aggregate = dict()
    jtids = [task["jeditaskid"] for task in tasks if task["status"] != "done"]
    for jtid in jtids:
        errors_dict = get_errors_from_jeditaskid(dbi, jtid)
        errors_aggregate[jtid] = errors_dict
    return errors_aggregate


class PandaChecker(SlurmChecker):  # pragma: no cover
    """Checker to use a slurm job_id and panda_id
    to check job status"""

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
        failed=StatusEnum.failed,
    )

    def check_url(self, dbi: DbInterface, job: JobBase) -> dict[str, Any]:
        update_vals: dict[str, Any] = {}
        if job.status not in [StatusEnum.populating, StatusEnum.running]:
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
        panda_status, errors_aggregate = check_panda_status(dbi, int(panda_url))
        if panda_status != job.panda_status:
            update_vals["panda_status"] = panda_status
        # Uncomment these lines to actually update the DB
        # Also remove the status = job.status line below
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
