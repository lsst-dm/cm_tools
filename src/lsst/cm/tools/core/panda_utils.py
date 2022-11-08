from typing import Any

from pandaclient import Client, panda_api

from lsst.cm.tools.core.db_interface import JobBase
from lsst.cm.tools.core.slurm_utils import SlurmChecker
from lsst.cm.tools.core.utils import StatusEnum


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
    conn = panda_api.get_api()
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

            # TODO: store the hecking pandaIDs so people can look things up

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

    def __init__(self):
        self.conn = panda_api.get_api()

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
        panda_status = self.check_panda_status(panda_url)
        if panda_status != job.panda_status:
            update_vals["panda_status"] = panda_status
        status = self.panda_status_map[panda_status]
        if status != job.status:
            update_vals["status"] = status
        return update_vals

    def check_panda_status(self, panda_url: str, panda_username=None) -> list[str]:
        """Check the status of a panda job

        Parameters
        ----------
        panda_url: str
            typically a reqid associated with the job
        panda_username: str
            None by default, username required for other submitters

        Returns
        -------
        job_statuses: list[str]
            list of status messages with associated task_id
        """
        # TODO: Fix to add in days argument to get around PanDA
        # storing for only two weeks
        tasks = self.conn.get_tasks(task_ids=panda_url, username=panda_username, days=90)
        job_statuses = [task["status"] for task in tasks]

        print("panda-url is {}".format(panda_url))
        print("panda-username is {}".format(panda_username))
        print(job_statuses)

        return job_statuses

    def check_task_errors(self, panda_url: str, panda_username=None):
        """Check the errors for a given panda reqid and
        return aggregated information

        Parameters
        ----------
        panda_url: str
            a reqid associated with the job
        panda_username: str
            None by default, username required for other submissions

        Returns
        -------
        errors_aggregate: dict
            a dict of a list of dicts because I am a criminal, but
            contains all the error codes from the jobs corresponding
            to tasks in this panda reqid
        diags_aggregate: dict
            as above, but with the diagnostic messages

        """
        # for a given reqid, return all our jeditaskids
        # TODO: add in a days argument
        tasks = self.conn.get_tasks(task_ids=panda_url, username=panda_username, days=90)
        jtids = [task["jeditaskid"] for task in tasks]

        errors_aggregate = dict()
        diags_aggregate = dict()
        for jtid in jtids:
            errors_all, diags_all = get_errors_from_jeditaskid(jtid)
            # TODO: something to aggregate better here
            # for now, we just slap it into a dict
            errors_aggregate[str(jtid)] = errors_all
            diags_aggregate[str(jtid)] = diags_all

        return errors_aggregate, diags_aggregate
