from lsst.cm.tools.core.utils import LevelEnum, StatusEnum


def test_level_enum() -> None:
    for key_ in list(LevelEnum.__members__.keys()):
        level = LevelEnum[key_]
        if level == LevelEnum.production:
            assert level.parent() is None
        else:
            assert level.parent().value == level.value - 1
        if level == LevelEnum.group:
            assert level.child() is None
        else:
            assert level.child().value == level.value + 1


def test_status_enum() -> None:
    for key_ in list(StatusEnum.__members__.keys()):
        status = StatusEnum[key_]
        assert status.bad() == (status.value < 0)
