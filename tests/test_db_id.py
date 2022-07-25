from lsst.cm.tools.core.db_interface import DbId
from lsst.cm.tools.core.utils import LevelEnum


def test_db_id() -> None:

    db_top_id = DbId()
    assert db_top_id.to_tuple() == (None, None, None, None, None)
    assert db_top_id.level() is None

    db_p_id = db_top_id.extend(LevelEnum.production, 0)
    assert db_p_id.to_tuple() == (0, None, None, None, None)
    assert db_p_id[LevelEnum.production] == 0
    assert db_p_id.level() == LevelEnum.production

    db_c_id = db_p_id.extend(LevelEnum.campaign, 0)
    assert db_c_id.to_tuple() == (0, 0, None, None, None)
    assert db_c_id[LevelEnum.production] == 0
    assert db_c_id[LevelEnum.campaign] == 0
    assert db_c_id.level() == LevelEnum.campaign

    db_s_id = db_c_id.extend(LevelEnum.step, 0)
    assert db_s_id.to_tuple() == (0, 0, 0, None, None)
    assert db_s_id[LevelEnum.production] == 0
    assert db_s_id[LevelEnum.campaign] == 0
    assert db_s_id[LevelEnum.step] == 0
    assert db_s_id.level() == LevelEnum.step

    db_g_id = db_s_id.extend(LevelEnum.group, 0)
    assert db_g_id.to_tuple() == (0, 0, 0, 0, None)
    assert db_g_id[LevelEnum.production] == 0
    assert db_g_id[LevelEnum.campaign] == 0
    assert db_g_id[LevelEnum.step] == 0
    assert db_g_id[LevelEnum.group] == 0
    assert db_g_id.level() == LevelEnum.group

    db_w_id = db_g_id.extend(LevelEnum.workflow, 0)
    assert db_w_id.to_tuple() == (0, 0, 0, 0, 0)
    assert db_w_id[LevelEnum.production] == 0
    assert db_w_id[LevelEnum.campaign] == 0
    assert db_w_id[LevelEnum.step] == 0
    assert db_w_id[LevelEnum.group] == 0
    assert db_w_id[LevelEnum.workflow] == 0
    assert db_w_id.level() == LevelEnum.workflow
