from typing import Any

from pandaclient import Client, panda_api

from lsst.cm.tools.core.db_interface import JobBase
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
    reqid_pull = conn.get_tasks(task_ids=reqid, username=username)
    jeditaskids = [reqid["jeditaskid"] for reqid in reqid_pull]

    return jeditaskids


def get_errors_from_jeditaskid(jeditaskid: int):
    """Return the errors associated with a jeditaskid as
    a dictionary for each job.

    Parameters
    ----------
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

    # grab all the PanDA IDs
    if conn_status == 0:
        job_ids = list(task_status["PandaID"])
        jobs_list = []
        if len(job_ids) > 0:
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
            print("no jobs found")
            return [], []
            # TODO: properly address this break condition,
            # because something went wrong

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
                error_dict["panda_err_code"] = "brokerage, " + job.brokerageErrorCode
                error_dict["diagnostic_message"] = job.brokerageErrorDiag
            # ddmErrorCode/Diag
            if job.ddmErrorCode != 0:
                error_dict["panda_err_code"] = "ddm, " + job.ddmErrorCode
                error_dict["diagnostic_message"] = job.ddmErrorDiag
            # exeErrorCode/Diag
            if job.exeErrorCode != 0:
                error_dict["panda_err_code"] = "exe, " + job.exeErrorCode
                error_dict["diagnostic_message"] = job.exeErrorDiag
            # jobDispatcherErrorCode/Diag
            if job.jobDispatcherErrorCode != 0:
                error_dict["panda_err_code"] = "jobDispatcher, " + job.jobDispatcherErrorCode
                error_dict["diagnostic_message"] = job.jobDispatcherErrorDiag
            # pilotErrorCode/Diag
            if job.pilotErrorCode != 0:
                error_dict["panda_err_code"] = "pilot, " + job.pilotErrorCode
                error_dict["diagnostic_message"] = job.pilotErrorDiag
            # supErrorCode/Diag
            if job.supErrorCode != 0:
                error_dict["panda_err_code"] = "sup, " + job.supErrorCode
                error_dict["diagnostic_message"] = job.supErrorDiag
            # taskBufferErrorCode/Diag
            if job.taskBufferErrorCode != 0:
                error_dict["panda_err_code"] = "taskBuffer, " + job.taskBufferErrorCode
                error_dict["diagnostic_message"] = job.taskBufferErrorDiag
            # transExitCode (no Diag)
            if job.transExitCode != 0:
                error_dict["panda_err_code"] = "trans, " + job.transErrorCode
                error_dict["diagnostic_message"] = "check the logging"
            error_dict["function"] = job.jobName.split("_")[-3]
            error_dict["log_file_url"] = job.pilotID.split("|")[0]
            # TODO: currently not found in Python
            # providing nearest substitute
            error_dict["data_id"] = (job.Files[0]).lfn
            error_dicts.append(error_dict)
        return error_dicts


def decide_panda_status(statuses: list) -> str:
    """Look at the list of statuses for each
    jeditaskid and return a choice for the entire
    reqid status

    Parameters
    ----------
    statuses: list
        a list of statuses for each jeditaskid
        in the reqid

    Returns
    -------
    panda_status: str
        the panda job status
    """
    # probably a better choice than using an elif
    # for this, but the elif lets us build
    # in possible options for intermediate steps.

    if "failed" in statuses:
        panda_status = "failed"
    elif "finished" in statuses:
        panda_status = "failed"
    elif "pending" in statuses:
        panda_status = "running"
    elif "registered" in statuses:
        panda_status = "running"
    elif "running" in statuses:
        panda_status = "running"
    else:
        panda_status = "done"
    return panda_status


def check_panda_status(panda_reqid: int, panda_username=None) -> str:
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

    """

    # first pull down all the tasks
    conn = panda_api.get_api()
    tasks = conn.get_tasks(task_ids=panda_reqid, username=panda_username)
    statuses = [task["status"] for task in tasks]

    # then pull all the errors for the tasks
    errors_aggregate = dict()
    jtids = [task["jeditaskid"] for task in tasks if task["status"] != "done"]
    for jtid in jtids:
        errors_aggregate[str(jtid)] = get_errors_from_jeditaskid(jtid)

    # now determine a final answer based on statuses for the entire reqid
    panda_status = decide_panda_status(statuses, errors_aggregate)

    return panda_status


def get_panda_errors(panda_reqid: int, panda_username=None) -> str:
    conn = panda_api.get_api()
    tasks = conn.get_tasks(task_ids=panda_reqid, username=panda_username)

    errors_aggregate = dict()
    jtids = [task["jeditaskid"] for task in tasks if task["status"] != "done"]
    for jtid in jtids:
        errors_dict = get_errors_from_jeditaskid(jtid)
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
        Running=StatusEnum.running,
    )

    panda_status_map = dict(
        Running=StatusEnum.running,
    )

    def check_url(self, job: JobBase) -> dict[str, Any]:
        update_vals: dict[str, Any] = {}
        panda_url = job.panda_url
        if panda_url is None:
            slurm_dict = SlurmChecker.check_url(self, job)
            if not slurm_dict:
                return update_vals
            batch_status = slurm_dict.get("batch_status", job.batch_status)
            if batch_status != job.batch_status:
                update_vals["batch_status"] = batch_status
            if slurm_dict.get("status") == StatusEnum.completed:
                bps_dict = parse_bps_stdout(job.log_url)
                panda_url = bps_dict["Run Id"]
                update_vals["panda_url"] = panda_url
        if panda_url is None:
            return update_vals
        panda_status = check_panda_status(panda_url)
        if panda_status != job.panda_status:
            update_vals["panda_status"] = panda_status
        status = self.panda_status_map[panda_status]
        if status != job.status:
            update_vals["status"] = status
        return update_vals

    @classmethod
    def check_panda_status(cls, panda_reqid: int, panda_username=None) -> str:
        return check_panda_status(panda_reqid, panda_username)

    @classmethod
    def get_panda_errors(cls, panda_reqid: int, panda_username=None) -> str:
        return get_panda_errors(panda_reqid, panda_username)
