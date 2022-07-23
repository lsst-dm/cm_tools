import enum
import os


class StatusEnum(enum.Enum):
    failed = -2  # Processing failed
    rejected = -1  # Marked as rejected
    waiting = 0  # Inputs are not ready
    preparing = 1  # Inputs are being prepared
    ready = 2  # Inputs are ready
    pending = 3  # Jobs are queued for submission
    running = 4  # Jobs are running
    collecting = 5  # Jobs have finshed running, collecting results
    completed = 6  # Completed, awaiting review
    accepted = 7  # Completed, reviewed and accepted
    superseded = 8  # Marked as superseded

    def ignore(self):
        """Can be used to filter out failed and rejected runs"""
        return self.value < 0


class LevelEnum(enum.Enum):
    production = 0  # A family of related campaigns
    campaign = 1  # A full data processing campaign
    step = 2  # Part of a campaign that is finished before moving on
    group = 3  # A subset of data that can be processed in paralllel as part of a step
    workflow = 4  # A single Panda workflow

    def parent(self):
        """Return the parent level, or `None` if does not exist"""
        if self.value == 0:
            return None
        return LevelEnum(self.value - 1)

    def child(self):
        """Return the child level, or `None` if does not exist"""
        if self.value == 4:
            return None
        return LevelEnum(self.value + 1)


def safe_makedirs(path):
    try:
        os.makedirs(path)
    except OSError:
        pass
