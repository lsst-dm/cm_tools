from typing import Any

from pandaclient import panda_api

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
        panda_status = self.heck_panda_status(panda_url)
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
        tasks = self.conn.get_tasks(task_ids=panda_url, username=panda_username)
        job_statuses = [task["status"] for task in tasks]

        print("panda-url is {}".format(panda_url))
        print("panda-username is {}".format(panda_username))
        print(job_statuses)

        return job_statuses
