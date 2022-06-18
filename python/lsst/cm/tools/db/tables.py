"""Defintion of database tables used in campaign management"""

import enum
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer, String, DateTime, Enum, ForeignKey

class StatusEnum(enum.Enum):
    waiting = 0,
    ready = 1,
    pending = 2,
    running = 3,
    failed = 4,
    done = 5,
    superseeded = 6
    pass
    

workflow_id = Column('id', Integer, primary_key = True)
group_id = Column('id', Integer, primary_key = True)
step_id = Column('id', Integer, primary_key = True)
campaign_id = Column('id', Integer, primary_key = True)
production_id = Column('id', Integer, primary_key = True)

production_name = Column('name', String)
campaign_name = Column('name', String)
step_name = Column('name', String)

production_idx = Column('idx', Integer)
campaign_idx = Column('idx', Integer)
step_idx = Column('idx', Integer)
group_idx = Column('idx', Integer)
workflow_idx = Column('idx', Integer)

time_start = Column('time_start', DateTime)
time_stop = Column('time_stop', DateTime)
time_cpu = Column('time_cpu', Float)

n_tasks_all = Column('n_tasks_all', Integer)
n_tasks_done = Column('n_tasks_done', Integer)
n_tasks_failed = Column('n_tasks_failed', Integer)

n_clusters_all = Column('n_clusters_all', Integer)
n_clusters_done = Column('n_clusters_done', Integer)
n_clusters_failed = Column('n_clusters_failed', Integer)

n_workflows = Column('n_workflows', Integer)

n_groups_all = Column('n_groups_all', Integer)
n_groups_done = Column('n_groups_done', Integer)
n_groups_failed = Column('n_groups_failed', Integer)

n_steps_all = Column('n_steps_all', Integer)
n_steps_done = Column('n_steps_done', Integer)
n_steps_failed = Column('n_steps_failed', Integer)

n_campaigns = Column('n_campaigns', Integer)

workflow_tmpl_url = Column('workflow_tmpl_url', String)
workflow_submitted_url = Column('workflow_submitted_url', String)

data_query_tmpl = Column('data_query_tmpl', String)
data_query_submitted = Column('data_query_submitted', String)

command_tmpl = Column('command_tmpl', String)
command_submitted = Column('command_submitted', String)
panda_log_url = Column('panda_log_url', String)
panda_status = Column('panda_status', Enum(StatusEnum))

workflow_coll_in= Column('workflow_coll_in', String)
workflow_coll_out = Column('workflow_coll_out', String)

group_coll_in= Column('group_coll_in', String)
group_coll_out = Column('group_coll_out', String)

step_coll_in= Column('step_coll_in', String)
step_coll_out = Column('step_coll_out', String)

campaign_coll_in = Column('campaign_coll_in', String)
campaign_coll_out = Column('campaign_coll_out', String)

group_status = Column('group_status', Enum(StatusEnum))
step_status = Column('step_status', Enum(StatusEnum))
campaign_status = Column('campaign_status', Enum(StatusEnum))


production_meta = MetaData()
production_table = Table('production', production_meta,
                         production_id,
                         production_name, 
                         production_idx,
                         n_campaigns)

campaign_meta = MetaData()
campaign_table = Table('campaign', campaign_meta,
                       campaign_id,
                       campaign_name, 
                       Column('production_idx', Integer), #ForeignKey("production.idx"), nullable=False),
                       campaign_idx,
                       n_steps_all,
                       n_steps_done,
                       n_steps_failed,
                       campaign_coll_in,
                       campaign_coll_out,
                       campaign_status)


step_meta = MetaData()
step_table = Table('step', step_meta,
                   step_id,
                   step_name, 
                   Column('production_idx', Integer), #ForeignKey("production.idx"), nullable=False),
                   Column('campaign_idx', Integer), #ForeignKey("campaign.idx"), nullable=False),
                   step_idx,
                   n_groups_all,
                   n_groups_done,
                   n_groups_failed,
                   step_coll_in,
                   step_coll_out,
                   step_status)

group_meta = MetaData()
group_table = Table('group', group_meta,
                    group_id,
                    Column('production_idx', Integer), #ForeignKey("production.idx"), nullable=False),
                    Column('campaign_idx', Integer), #ForeignKey("campaign.idx"), nullable=False),
                    Column('step_idx', Integer), #ForeignKey("step.idx"), nullable=False),
                    group_idx,
                    n_workflows,
                    group_coll_in,
                    group_coll_out,
                    group_status)

workflow_meta = MetaData()
workflow_table = Table('workflow',
                       workflow_meta,
                       workflow_id,
                       Column('production_idx', Integer), #ForeignKey("production.idx"), nullable=False),
                       Column('campaign_idx', Integer), #ForeignKey("campaign.idx"), nullable=False),
                       Column('step_idx', Integer), #ForeignKey("step.idx"), nullable=False),
                       Column('group_idx', Integer), #ForeignKey("group.idx"), nullable=False),
                       workflow_idx,
                       time_start,
                       time_stop,
                       time_cpu,
                       n_tasks_all,
                       n_tasks_done,
                       n_tasks_failed,
                       n_clusters_all,
                       n_clusters_done,
                       n_clusters_failed,
                       workflow_tmpl_url,
                       workflow_submitted_url,
                       data_query_tmpl,
                       data_query_submitted,
                       command_tmpl,
                       command_submitted,
                       panda_log_url,
                       panda_status,                       
                       workflow_coll_in,
                       workflow_coll_out)


def create_db(engine):

    from sqlalchemy_utils import create_database
    create_database(engine.url)
    for meta in [production_meta, campaign_meta, step_meta, group_meta, workflow_meta]:
        meta.create_all(engine)            



if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(prog='tables.py')

    parser.add_argument('--db', type=str, help='Database', default="sqlite:///cm.db")
    parser.add_argument('--create', action='store_true', default=False, help="Create DB")

    args = parser.parse_args()

    engine = create_engine(args.db, echo=True)
    from sqlalchemy_utils import database_exists, create_database
    
    if not database_exists(engine.url):
        if args.create:
            create_db(engine)

    if not database_exists(engine.url):
        raise RuntimeError(f'Failed to access or create database {args.db}')
        
