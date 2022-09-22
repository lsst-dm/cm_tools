from typing import Any

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


def check_panda_status(panda_url: str) -> str:
    """Check the status of a panda job"""
    return "Running"


class PandaChecker(SlurmChecker):  # pragma: no cover
    """Checker to use a slurm job_id and panda_id to check job status"""

    def check_url(self, job: JobBase) -> dict[str, Any]:
        update_vals = {}
        panda_url = job.panda_url
        if panda_url is None:
            slurm_dict = SlurmChecker.check_url(self, job)
            batch_status = slurm_dict.get("batch_status", job.batch_status)
            if batch_status != job.batch_status:
                update_vals["batch_status"] = batch_status
            if slurm_dict["status"] == StatusEnum.completed:
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
