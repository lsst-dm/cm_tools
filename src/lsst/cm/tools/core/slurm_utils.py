import subprocess
from typing import Any

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.db_interface import ScriptBase
from lsst.cm.tools.core.utils import StatusEnum


def submit_job(job_path: str, log_path: str) -> str:  # pragma: no cover
    """Submit a job to slurm and return the job_id

    Parameters
    ----------
    job_path : str
        Path to a bash script to run the job

    log_path : str
        Path to a log file

    Returns
    -------
    job_id : str
        The slurm job_id
    """
    with subprocess.Popen(
        ["sbatch", "-o", log_path, "--mem", "16448", "-p", "roma", "--parsable", job_path],
        stdout=subprocess.PIPE,
    ) as sbatch:
        assert sbatch.stdout
        line = sbatch.stdout.read().decode().strip()
        job_id = line.split("|")[0]
    return job_id


def check_job_status(job_id: str | None) -> str:  # pragma: no cover
    """Check the status of a slurm job

    Parameters
    ----------
    job_id : str
        The slurm job_id

    Returns
    -------
    job_status : str
        The slurm job status
    """
    if job_id is None:
        return "NOT_SUBMITTED"
    with subprocess.Popen(["sacct", "--parsable", "-b", "-j", job_id], stdout=subprocess.PIPE) as sacct:
        assert sacct.stdout
        lines = sacct.stdout.read().decode().split("\n")
        if len(lines) < 2:
            return "PENDING"
        tokens = lines[1].split("|")
        if len(tokens) < 2:
            return "PENDING"
        job_status = tokens[1]
    return job_status


class SlurmChecker(Checker):  # pragma: no cover
    """Simple Checker to use a slurm job_id to check job status"""

    status_map = dict(
        BOOT_FAIL=StatusEnum.failed,
        CANCELLED=StatusEnum.failed,
        COMPLETED=StatusEnum.completed,
        CONFIGURING=StatusEnum.preparing,
        COMPLETING=StatusEnum.running,
        DEADLINE=StatusEnum.failed,
        FAILED=StatusEnum.failed,
        NODE_FAIL=StatusEnum.failed,
        NOT_SUBMITTED=StatusEnum.ready,
        OUT_OF_MEMORY=StatusEnum.failed,
        PENDING=StatusEnum.preparing,
        PREEMPTED=StatusEnum.running,
        RUNNING=StatusEnum.running,
        RESV_DEL_HOLD=StatusEnum.running,
        REQUEUE_FED=StatusEnum.running,
        REQUEUE_HOLD=StatusEnum.running,
        REQUEUED=StatusEnum.running,
        RESIZING=StatusEnum.running,
        REVOKED=StatusEnum.failed,
        SIGNALING=StatusEnum.running,
        SPECIAL_EXIT=StatusEnum.failed,
        STAGE_OUT=StatusEnum.running,
        STOPPED=StatusEnum.running,
        SUSPENDED=StatusEnum.running,
        TIMEOUT=StatusEnum.failed,
    )

    def check_url(self, script: ScriptBase) -> dict[str, Any]:
        new_values: dict[str, Any] = {}
        if script.stamp_url is None:
            return new_values
        slurm_status = check_job_status(script.stamp_url)
        if slurm_status != script.batch_status:
            new_values["batch_status"] = slurm_status
        status = self.status_map[slurm_status]
        if status != script.status:
            new_values["status"] = status
        return new_values
