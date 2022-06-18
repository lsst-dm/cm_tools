
from sqlalchemy import create_engine, select, and_
from tables import create_db, production_table, campaign_table, step_table, group_table, workflow_table, StatusEnum


def get_production_table_length(conn):
    #FIXME, better way to do this
    sel = production_table.select()
    sel_result = conn.execute(sel)
    return len(sel_result.all())


def get_production_idx(conn, production_name):
    #FIXME, better way to do this
    sel = select([production_table.c.idx]).where(production_table.c.name == production_name)
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]


def print_production_table(conn):
    sel = production_table.select()
    sel_result = conn.execute(sel)
    for row in sel_result:
        print(row)


def create_production(conn, production_name):
    production_idx = get_production_table_length()
    ins = production_table.insert().values(name=production_name, idx=production_idx)
    ins_result = conn.execute(ins)    
    print_production_table(conn)


def get_campaign_idx(conn, production_idx, campaign_name):
    #FIXME, better way to do this
    sel = select([campaign_table.c.idx]).where(and_(campaign_table.c.name == campaign_name,
                                                    campaign_table.c.production_idx == production_idx))
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]

def get_campaign_number(conn, production_name):
    sel = select([production_table.c.n_campaigns]).where(production_table.c.name == production_name)
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]

def print_campaigns(conn, production_idx):
    sel = campaign_table.select().where(campaign_table.c.production_idx == production_idx)
    sel_result = conn.execute(sel)

    for row in sel_result:
        print(row)

def _create_campaign(conn, production_idx, campaign_idx, campaign_name, **kwargs):
    ins_values = dict(name=campaign_name,
                      production_idx=production_idx,
                      idx=campaign_idx,
                      n_steps_all=0,
                      n_steps_done=0,
                      n_steps_failed=0,
                      campaign_coll_in="",
                      campaign_coll_out="",
                      campaign_status=StatusEnum.waiting)    
    ins_values.update(**kwargs)    
    ins = campaign_table.insert().values(**ins_values)
    ins_result = conn.execute(ins)    
    stmt = production_table.update().where(production_table.c.idx == production_idx).values(n_campaigns=campaign_idx+1)
    upd_result = conn.execute(stmt)


def print_campaigns(conn, production_idx):
    sel = campaign_table.select().where(campaign_table.c.production_idx == production_idx)
    sel_result = conn.execute(sel)

    for row in sel_result:
        print(row)    

    
def create_campaign(conn, production_name, campaign_name, **kwargs):    
    production_idx = get_production_idx(conn, production_name)
    campaign_idx = get_campaign_number(conn, production_name)
    _create_campaign(conn, production_idx, campaign_idx, campaign_name, **kwargs)    
    print_campaigns(conn, production_idx)


def get_step_idx(conn, production_idx, campaign_idx, step_name):
    sel = step_table.select().where(and_(step_table.c.production_idx == production_idx,
                                         step_table.c.campaign_idx == campaign_idx,
                                         step_table.c.name == step_name))
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]
   

def get_step_number(conn, production_idx, campaign_idx):
    sel = select([campaign_table.c.n_step_all]).where(_and(campaign_table.c.production_idx == production_idx,
                                                           campaign_table.c.idx == campaign_idx))
    sel_result = conn.execute(sel)
    return sel_result.all()[0][0]


def _create_step(conn, production_idx, campaign_idx, step_idx, step_name, **kwargs)
    ins_values = dict(name=step_name,
                      production_idx=production_idx,
                      campaign_idx=campaign_idx,
                      idx=step_idx,                      
                      n_groups_all=0,
                      n_groups_done=0,
                      n_groups_failed=0,
                      step_coll_in="",
                      step_coll_out="",
                      step_status=StatusEnum.waiting)    
    ins_values.update(**kwargs)
    ins = step_table.insert().values(**ins_values)
    ins_result = conn.execute(ins)

    stmt = campaign_table.update().where(and_(campaign_table.c.production_idx == production_idx,
                                              campaign_table.c.idx == campaign_idx)).values(n_steps_all=step_idx+1)
    conn.execute(stmt)


def print_steps(conn, production_idx, campaign_idx):
    sel = step_table.select().where(and_(step_table.c.production_idx == production_idx,
                                         step_table.c.campaign_idx == campaign_idx))
    sel_result = conn.execute(sel)

    for row in sel_result:
        print(row)

    
        
def create_step(conn, production_name, campaign_name, step_name):
    production_idx = get_production_idx(conn, production_name)
    campaign_idx = get_campaign_idx(conn, production_idx, campaign_name)
    step_idx = get_step_number(conn, production_idx, campaign_idx)
    _create_step(conn, production_idx, campaign_idx, step_idx, step_name, **kwargs)    
    print_steps(conn, production_idx, campaign_idx)


def _create_groups(conn, production_idx, campaign_idx, step_idx, ngroups, **kwargs):
    ins_values = dict(name=step_name,
                      production_idx=production_idx,
                      campaign_idx=campaign_idx,
                      step_idx=step_idx,
                      idx=0,                      
                      n_workflows=0,
                      group_coll_in="",
                      group_coll_out="",
                      group_status=StatusEnum.waiting)    
    ins_values.update(**kwargs)    
    for i in range(ngroups):
        ins_values.update(idx=i)
        ins = step_table.insert().values(**ins_values)
        ins_result = conn.execute(ins)
    stmt = step_table.update().where(and_(step_table.c.production_idx == production_idx,
                                          step_table.c.campaign_idx == campaign_idx,
                                          step_table.c.idx == step_idx)).values(n_groups_all=ngroups)
    conn.execute(stmt)


def print_groups(conn, production_idx, campaign_idx, step_idx):
    sel = group_table.select().where(and_(group_table.c.production_idx == production_idx,
                                          group_table.c.campaign_idx == campaign_idx,
                                          group_table.c.step_idx == step_idx))
    sel_result = conn.execute(sel)
    for row in sel_result:
        print(row)

        
    
def create_groups(conn, production_name, campaign_name, step_name, ngroups):    
    production_idx = get_production_idx(conn, production_name)
    campaign_idx = get_campaign_idx(conn, production_idx, campaign_name)
    step_idx = get_step_idx(conn, production_idx, campaign_idx, step_name)
    _create_groups(conn, production_idx, campaign_idx, step_idx, ngroups, **kwargs)
    print_groups(conn, production_idx, campaign_idx, step_idx)


def _create_workflows(conn, production_idx, campaign_idx, step_idx, ngroups, **kwargs):
    ins_values = dict(name=step_name,
                      production_idx=production_idx,
                      campaign_idx=campaign_idx,
                      step_idx=step_idx,
                      group_idx=0,
                      idx=0,
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
                      panda_status=StatusEnum.waiting,                       
                      workflow_coll_in="",
                      workflow_coll_out="")    
    ins_values.update(**kwargs)    
    for i in range(ngroups):
        ins_values.update(group_idx=i)
        ins = workflow_table.insert().values(**ins_values)
        ins_result = conn.execute(ins)
        stmt = group_table.update().where(and_(group_table.c.production_idx == production_idx,
                                               group_table.c.campaign_idx == campaign_idx,
                                               group_table.c.step_idx == step_idx,
                                               group_table.c.idx == i)).values(n_workflows=1)
        conn.execute(stmt)
    


def print_workflows(conn, production_idx, campaign_idx, step_idx):
    sel = workflow_table.select().where(and_(workflow_table.c.production_idx == production_idx,
                                             workflow_table.c.campaign_idx == campaign_idx,
                                             workflow_table.c.step_idx == step_idx))
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
    parser.add_argument('--production_name', type=str, help="Production Name", required=True)
    parser.add_argument('--campaign_name', type=str, help="Campaign Name", default=None)
    parser.add_argument('--step_name', type=str, help="Step Name", default=None)
    parser.add_argument('--ngroup', type=int, help="Number of groups in step, default=None)        
    parser.add_argument('--echo', action='store_true', default=False, help="Echo DB commands")
    
    args = parser.parse_args()

    engine = create_engine(args.db, echo=args.echo)
    from sqlalchemy_utils import database_exists, create_database
    
    if not database_exists(engine.url):
        if args.create:
            create_db(engine)
            
    if not database_exists(engine.url):
        raise RuntimeError(f'Failed to access or create database {args.db}')
        
    conn = engine.connect()

    if args.campaign_name is None:
        create_production(conn, args.production_name)
    elif args.step_name is None:
        create_campaign(conn, args.production_name, args.campaign_name)
    elif args.ngroups is None:
        create_step(conn, args.production_name, args.campaign_name, args.step_name)
    else: 
        create_step(conn, args.production_name, args.campaign_name, args.step_name, args.ngroups)
        create_workflows(conn, args.production_name, args.campaign_name, args.step_name)
