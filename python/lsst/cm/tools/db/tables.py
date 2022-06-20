"""Defintion of database tables used in campaign management"""

import enum
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer, String, DateTime, Enum


class StatusEnum(enum.Enum):
    waiting = 0,
    ready = 1,
    pending = 2,
    running = 3,
    failed = 4,
    done = 5,
    superseeded = 6
    pass


production_meta = MetaData()
production_table = Table(
    'production', production_meta,
    Column('id', Integer, primary_key=True),
    Column('p_id', Integer),
    Column('name', String),
    Column('n_campaigns', Integer)
)

campaign_meta = MetaData()
campaign_table = Table(
    'campaign', campaign_meta,
    Column('id', Integer, primary_key=True),
    Column('p_id', Integer),
    Column('c_id', Integer),
    Column('name', String),
    Column('n_steps_all', Integer),
    Column('n_steps_done', Integer),
    Column('n_steps_failed', Integer),
    Column('c_coll_in', String),
    Column('c_coll_out', String),
    Column('c_status', Enum(StatusEnum))
)

step_meta = MetaData()
step_table = Table(
    'step', step_meta,
    Column('id', Integer, primary_key=True),
    Column('p_id', Integer),
    Column('c_id', Integer),
    Column('s_id', Integer),
    Column('name', String),
    Column('n_groups_all', Integer),
    Column('n_groups_done', Integer),
    Column('n_groups_failed', Integer),
    Column('s_coll_in', String),
    Column('s_coll_out', String),
    Column('s_status', Enum(StatusEnum))
)

group_meta = MetaData()
group_table = Table(
    'group', group_meta,
    Column('id', Integer, primary_key=True),
    Column('p_id', Integer),
    Column('c_id', Integer),
    Column('s_id', Integer),
    Column('g_id', Integer),
    Column('n_workflows', Integer),
    Column('g_coll_in', String),
    Column('g_coll_out', String),
    Column('g_status', Enum(StatusEnum))
)

workflow_meta = MetaData()
workflow_table = Table(
    'workflow', workflow_meta,
    Column('id', Integer, primary_key=True),
    Column('p_id', Integer),
    Column('c_id', Integer),
    Column('s_id', Integer),
    Column('g_id', Integer),
    Column('w_id', Integer),
    Column('n_tasks_all', Integer),
    Column('n_tasks_done', Integer),
    Column('n_tasks_failed', Integer),
    Column('n_clusters_all', Integer),
    Column('n_clusters_done', Integer),
    Column('n_clusters_failed', Integer),
    Column('workflow_start', DateTime),
    Column('workflow_end', DateTime),
    Column('workflow_cputime', Float),
    Column('workflow_tmpl_url', String),
    Column('workflow_submitted_url', String),
    Column('data_query_tmpl', String),
    Column('data_query_submitted', String),
    Column('command_tmpl', String),
    Column('command_submitted', String),
    Column('panda_log_url', String),
    Column('w_coll_in', String),
    Column('w_coll_out', String),
    Column('w_status', Enum(StatusEnum))
)


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
    from sqlalchemy_utils import database_exists

    if not database_exists(engine.url):
        if args.create:
            create_db(engine)

    if not database_exists(engine.url):
        raise RuntimeError(f'Failed to access or create database {args.db}')
