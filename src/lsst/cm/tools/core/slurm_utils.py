import subprocess


def submit_job(job_path: str) -> str:  # pragma: no cover
    """Submit a job to slurm and return the job_id

    Parameters
    ----------
    job_path : str
        Path to a bash script to run the job

    Returns
    -------
    job_id : str
        The slurm job_id
    """
    with subprocess.Popen(["sbatch", "--parsable", job_path]) as sbatch:
        lines = sbatch.stdout.read()
        job_id = lines[0].split("|")[0]
    return job_id


def check_job_status(job_id: str) -> str:  # pragma: no cover
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
    with subprocess.Popen(["sacct", "--parsable", "-b", "-j", job_id]) as sacct:
        lines = sacct.stdout.read()
        job_status = lines[1].split("|")[1]
    return job_status
