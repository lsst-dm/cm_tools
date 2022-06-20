import sys
from sqlalchemy import create_engine, select, and_, func
from tables import create_db, production_table, campaign_table, step_table,\
    group_table, workflow_table, StatusEnum


def get_production_table_length(conn):
    count = func.count(production_table.c.p_id)
    return conn.execute(count).scalar()


def get_production_idx(conn, production_name):
    sel = select([production_table.c.p_id]).where(production_table.c.name == production_name)
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]


def print_production_table(conn):
    sel = production_table.select()
    sel_result = conn.execute(sel)
    for row in sel_result:
        print(row)


def create_production(conn, production_name):
    production_idx = get_production_table_length(conn)
    ins = production_table.insert().values(name=production_name, p_id=production_idx, n_campaigns=0)
    ins_result = conn.execute(ins)
    print_production_table(conn)
    return ins_result


def get_campaign_idx(conn, production_idx, campaign_name):
    sel = select([campaign_table.c.c_id]).where(and_(campaign_table.c.name == campaign_name,
                                                     campaign_table.c.p_id == production_idx))
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]


def get_campaign_number(conn, production_name):
    sel = select([production_table.c.n_campaigns]).where(production_table.c.name == production_name)
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]


def _create_campaign(conn, production_idx, campaign_idx, campaign_name, **kwargs):
    ins_values = dict(name=campaign_name,
                      p_id=production_idx,
                      c_id=campaign_idx,
                      n_steps_all=0,
                      n_steps_done=0,
                      n_steps_failed=0,
                      c_coll_in="",
                      c_coll_out="",
                      c_status=StatusEnum.waiting)
    ins_values.update(**kwargs)
    ins = campaign_table.insert().values(**ins_values)
    conn.execute(ins)
    stmt = production_table.update().where(
        production_table.c.p_id == production_idx).values(n_campaigns=campaign_idx+1)
    upd_result = conn.execute(stmt)
    return upd_result


def print_campaigns(conn, production_idx):
    sel = campaign_table.select().where(campaign_table.c.p_id == production_idx)
    sel_result = conn.execute(sel)
    for row in sel_result:
        print(row)


def create_campaign(conn, production_name, campaign_name, **kwargs):
    production_idx = get_production_idx(conn, production_name)
    campaign_idx = get_campaign_number(conn, production_name)
    upd_result = _create_campaign(conn, production_idx, campaign_idx, campaign_name, **kwargs)
    print_campaigns(conn, production_idx)
    return upd_result


def get_step_idx(conn, production_idx, campaign_idx, step_name):
    sel = step_table.select().where(and_(step_table.c.p_id == production_idx,
                                         step_table.c.c_id == campaign_idx,
                                         step_table.c.name == step_name))
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]


def get_step_number(conn, production_idx, campaign_idx):
    sel = select([campaign_table.c.n_steps_all]).where(and_(campaign_table.c.p_id == production_idx,
                                                            campaign_table.c.c_id == campaign_idx))
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]


def _create_step(conn, production_idx, campaign_idx, step_idx, step_name, **kwargs):
    ins_values = dict(name=step_name,
                      p_id=production_idx,
                      c_id=campaign_idx,
                      s_id=step_idx,
                      n_groups_all=0,
                      n_groups_done=0,
                      n_groups_failed=0,
                      s_coll_in="",
                      s_coll_out="",
                      s_status=StatusEnum.waiting)
    ins_values.update(**kwargs)
    ins = step_table.insert().values(**ins_values)
    conn.execute(ins)
    stmt = campaign_table.update().where(and_(campaign_table.c.p_id == production_idx,
                                              campaign_table.c.c_id == campaign_idx)).\
        values(n_steps_all=step_idx+1)
    upd_result = conn.execute(stmt)
    return upd_result


def print_steps(conn, production_idx, campaign_idx):
    sel = step_table.select().where(and_(step_table.c.p_id == production_idx,
                                         step_table.c.c_id == campaign_idx))
    sel_result = conn.execute(sel)
    for row in sel_result:
        print(row)


def create_step(conn, production_name, campaign_name, step_name, **kwargs):
    production_idx = get_production_idx(conn, production_name)
    campaign_idx = get_campaign_idx(conn, production_idx, campaign_name)
    step_idx = get_step_number(conn, production_idx, campaign_idx)
    upd_result = _create_step(conn, production_idx, campaign_idx, step_idx, step_name, **kwargs)
    print_steps(conn, production_idx, campaign_idx)
    return upd_result


def get_ngroups(conn, production_idx, campaign_idx, step_idx):
    count = func.count(and_(group_table.c.p_id == production_idx,
                            group_table.c.c_id == campaign_idx,
                            group_table.c.s_id == step_idx))
    return conn.execute(count).scalar()


def _create_groups(conn, production_idx, campaign_idx, step_idx, ngroups, **kwargs):
    ins_values = dict(p_id=production_idx,
                      c_id=campaign_idx,
                      s_id=step_idx,
                      g_id=0,
                      n_workflows=0,
                      g_coll_in="",
                      g_coll_out="",
                      g_status=StatusEnum.waiting)
    ins_values.update(**kwargs)
    n_group_start = get_ngroups(conn, production_idx, campaign_idx, step_idx)
    for i in range(n_group_start, n_group_start+ngroups):
        ins_values.update(g_id=i)
        ins = group_table.insert().values(**ins_values)
        conn.execute(ins)
    stmt = step_table.update().where(and_(step_table.c.p_id == production_idx,
                                          step_table.c.c_id == campaign_idx,
                                          step_table.c.s_id == step_idx)).values(n_groups_all=ngroups)
    upd_result = conn.execute(stmt)
    return upd_result


def print_groups(conn, production_idx, campaign_idx, step_idx):
    sel = group_table.select().where(and_(group_table.c.p_id == production_idx,
                                          group_table.c.c_id == campaign_idx,
                                          group_table.c.s_id == step_idx))
    sel_result = conn.execute(sel)
    for row in sel_result:
        print(row)


def create_groups(conn, production_name, campaign_name, step_name, ngroups, **kwargs):
    production_idx = get_production_idx(conn, production_name)
    campaign_idx = get_campaign_idx(conn, production_idx, campaign_name)
    step_idx = get_step_idx(conn, production_idx, campaign_idx, step_name)
    upd_result = _create_groups(conn, production_idx, campaign_idx, step_idx, ngroups, **kwargs)
    print_groups(conn, production_idx, campaign_idx, step_idx)
    return upd_result


def _create_workflows(conn, production_idx, campaign_idx, step_idx, ngroups, **kwargs):
    ins_values = dict(p_id=production_idx,
                      c_id=campaign_idx,
                      s_id=step_idx,
                      g_id=0,
                      w_id=0,
                      n_tasks_all=0,
                      n_tasks_done=0,
                      n_tasks_failed=0,
                      n_clusters_all=0,
                      n_clusters_done=0,
                      n_clusters_failed=0,
                      workflow_tmpl_url="",
                      workflow_submitted_url="",
                      data_query_tmpl="",
                      data_query_submitted="",
                      command_tmpl="",
                      command_submitted="",
                      panda_log_url="",
                      w_coll_in="",
                      w_coll_out="",
                      w_status=StatusEnum.waiting)
    ins_values.update(**kwargs)
    for i in range(ngroups):
        ins_values.update(g_id=i)
        ins = workflow_table.insert().values(**ins_values)
        conn.execute(ins)
        stmt = group_table.update().where(and_(group_table.c.p_id == production_idx,
                                               group_table.c.c_id == campaign_idx,
                                               group_table.c.s_id == step_idx,
                                               group_table.c.g_id == i)).values(n_workflows=1)
        conn.execute(stmt)


def print_workflows(conn, production_idx, campaign_idx, step_idx):
    sel = workflow_table.select().where(and_(workflow_table.c.p_id == production_idx,
                                             workflow_table.c.c_id == campaign_idx,
                                             workflow_table.c.s_id == step_idx))
    sel_result = conn.execute(sel)
    for row in sel_result:
        print(row)


def create_workflows(conn, production_name, campaign_name, step_name):
    production_idx = get_production_idx(conn, production_name)
    campaign_idx = get_campaign_idx(conn, production_idx, campaign_name)
    step_idx = get_step_idx(conn, production_idx, campaign_idx, step_name)
    ngroups = get_ngroups(conn, production_idx, campaign_idx, step_idx)
    _create_workflows(conn, production_idx, campaign_idx, step_idx, ngroups)
    print_workflows(conn, production_idx, campaign_idx, step_idx)


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(prog='tables.py')

    parser.add_argument('--db', type=str, help='Database', default="sqlite:///cm.db")
    parser.add_argument('--create', action='store_true', default=False, help="Create DB")
    parser.add_argument('--production_name', type=str, help="Production Name", default=None)
    parser.add_argument('--campaign_name', type=str, help="Campaign Name", default=None)
    parser.add_argument('--step_name', type=str, help="Step Name", default=None)
    parser.add_argument('--ngroup', type=int, help="Number of groups in step, default=None")
    parser.add_argument('--echo', action='store_true', default=False, help="Echo DB commands")

    args = parser.parse_args()

    engine = create_engine(args.db, echo=args.echo)
    from sqlalchemy_utils import database_exists

    if not database_exists(engine.url):
        if args.create:
            create_db(engine)

    if not database_exists(engine.url):
        raise RuntimeError(f'Failed to access or create database {args.db}')

    conn = engine.connect()

    if args.production_name is None:
        sys.exit(0)
    elif args.campaign_name is None:
        create_production(conn, args.production_name)
    elif args.step_name is None:
        create_campaign(conn, args.production_name, args.campaign_name)
    elif args.ngroup is None:
        create_step(conn, args.production_name, args.campaign_name, args.step_name)
    else:
        create_groups(conn, args.production_name, args.campaign_name, args.step_name, args.ngroup)
        create_workflows(conn, args.production_name, args.campaign_name, args.step_name)
