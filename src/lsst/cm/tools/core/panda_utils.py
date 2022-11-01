from typing import Any

from pandaclient import Client, panda_api

from lsst.cm.tools.core.db_interface import JobBase
from lsst.cm.tools.core.slurm_utils import SlurmChecker
from lsst.cm.tools.core.utils import StatusEnum


def check_panda_conn():
    """Check for existing PanDA connection

    Returns
    -------
    conn: PandaAPI
        connection to the PanDA API for calls
    """
    try:
        conn
    except NameError:
        conn = panda_api.get_api()
        statuscode, diagmess = conn.hello()
    # TODO: add handling with status code for authorization

    return conn


def parse_bps_stdout(url: str) -> dict[str, str]:
    """Parse the std from a bps submit job"""
    out_dict = {}
    with open(url, "r") as fin:
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
    conn = check_panda_conn()
    reqid_pull = conn.get_tasks(task_ids=reqid, username=username)
    jeditaskids = [reqid["jeditaskid"] for reqid in reqid_pull]

    return jeditaskids


def get_errors_from_jeditaskid(jeditaskid: int):
    """Return the errors associated with a jeditaskid

    Parameters
    ----------
    jeditaskid: int
        A jeditaskid, which will have some number of pandaIDs associated.

    Returns
    -------
    error_codes: list[dict]
        A list of dictionaries matching error code category
        to the returned value.
    error_diags: list[dict]
        A list of dictionaries matching error code categories
        to the associated diagnostic messages.
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
    errors_all = []
    diags_all = []

    failed_jobs = [job for job in jobs_list if job.jobStatus == "failed"]
    if len(failed_jobs) == 0:
        return (errors_all, diags_all)
    else:
        for job in failed_jobs:
            errors = dict()
            diags = dict()

            # brokerageErrorCode/Diag
            if job.brokerageErrorCode != 0:
                errors["brokerage"] = job.brokerageErrorCode
                diags["brokerage"] = job.brokerageErrorDiag
            # ddmErrorCode/Diag
            if job.ddmErrorCode != 0:
                errors["ddm"] = job.ddmErrorCode
                diags["ddm"] = job.ddmErrorDiag
            # exeErrorCode/Diag
            if job.exeErrorCode != 0:
                errors["exe"] = job.exeErrorCode
                diags["exe"] = job.exeErrorDiag
            # jobDispatcherErrorCode/Diag
            if job.jobDispatcherErrorCode != 0:
                errors["jobDispatcher"] = job.jobDispatcherErrorCode
                diags["jobDispatcher"] = job.jobDispatcherErrorDiag
            # pilotErrorCode/Diag
            if job.pilotErrorCode != 0:
                errors["pilot"] = job.pilotErrorCode
                diags["pilot"] = job.pilotErrorDiag
            # supErrorCode/Diag
            if job.supErrorCode != 0:
                errors["sup"] = job.supErrorCode
                diags["sup"] = job.supErrorDiag
            # taskBufferErrorCode/Diag
            if job.taskBufferErrorCode != 0:
                errors["taskBuffer"] = job.taskBufferErrorCode
                diags["taskBuffer"] = job.taskBufferErrorDiag
            # transExitCode (no Diag)
            if job.transExitCode != 0:
                errors["trans"] = job.transExitCode
                diags["trans"] = "check the logs"
            errors_all.append(errors)
            diags_all.append(diags)
        return (errors_all, diags_all)


class PandaChecker(SlurmChecker):  # pragma: no cover
    """Checker to use a slurm job_id and panda_id to check job status"""

    def check_url(self, job: JobBase) -> dict[str, Any]:
        update_vals = {}
        panda_url = job.panda_url
        if panda_url is None:
            slurm_dict = SlurmChecker.check_url(self, job)
            if not slurm_dict:
                return update_vals
            batch_status = slurm_dict.get("batch_status", job.batch_status)
            if batch_status != job.batch_status:
                update_vals["batch_status"] = batch_status
            if slurm_dict["status"] == StatusEnum.completed:
                bps_dict = parse_bps_stdout(job.log_url)
                panda_url = bps_dict["Run Id"]
                update_vals["panda_url"] = panda_url
        if panda_url is None:
            return update_vals
        # panda_status = check_panda_status(panda_url)
        # if panda_status != job.panda_status:
        #    update_vals["panda_status"] = panda_status
        # status = self.panda_status_map[panda_status]
        # if status != job.status:
        #    update_vals["status"] = status
        return update_vals
