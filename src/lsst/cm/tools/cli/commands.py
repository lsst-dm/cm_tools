import sys
from typing import Any

import click

from ..core.handler import Handler
from ..core.utils import LevelEnum, StatusEnum, TableEnum
from ..db.sqlalch_interface import SQLAlchemyInterface
from .options import (
    butler_option,
    campaign_option,
    config_option,
    data_query_option,
    db_option,
    echo_option,
    fullname_option,
    group_option,
    handler_option,
    level_option,
    max_running_option,
    nosubmit_option,
    prod_base_option,
    production_option,
    status_option,
    step_option,
    table_option,
    workflow_option,
)


@click.group()
def cli() -> None:
    """campaign management tool"""
    pass


@cli.command()
@db_option()
@echo_option()
def create(db: str, echo: bool) -> None:
    """create backing database"""
    SQLAlchemyInterface(db, echo=echo, create=True)


@cli.command()
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@data_query_option()
@butler_option()
@prod_base_option()
@workflow_option()
@handler_option()
@config_option()
@db_option()
@nosubmit_option()
@echo_option()
def insert(
    level: LevelEnum, db: str, handler: str, config_yaml: str, no_submit: bool, echo: bool, **kwargs: Any
) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    if level != LevelEnum.production:
        assert config_yaml is not None
        assert handler is not None
        the_handler = Handler.get_handler(handler, config_yaml)
        the_db_id = iface.get_db_id(level, **kwargs)
    else:
        the_db_id = None
        the_handler = None
    iface.insert(the_db_id, the_handler, **kwargs)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
def print_tree(level: LevelEnum, db: str, echo: bool, **kwargs: Any) -> None:
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.print_tree(sys.stdout, level, the_db_id)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
def print(level: LevelEnum, db: str, echo: bool, **kwargs: Any) -> None:
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.print_(sys.stdout, level, the_db_id)


@cli.command()
@table_option()
@db_option()
@echo_option()
def print_table(table: TableEnum, db: str, echo: bool) -> None:
    iface = SQLAlchemyInterface(db, echo=echo)
    iface.print_table(sys.stdout, table)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@nosubmit_option()
@echo_option()
def prepare(level: LevelEnum, db: str, no_submit: bool, echo: bool, **kwargs: Any) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    id_args = [
        "production_name",
        "campaign_name",
        "step_name",
        "group_name",
    ]
    all_args = kwargs.copy()
    for arg_ in id_args:
        all_args.pop(arg_)
    iface.prepare(level, the_db_id, **all_args)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
def queue(level: LevelEnum, db: str, echo: bool, **kwargs: Any) -> None:
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.queue_jobs(level, the_db_id)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
@nosubmit_option()
@max_running_option()
def launch(level: LevelEnum, db: str, echo: bool, no_submit: bool, max_running: int, **kwargs: Any) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.launch_jobs(level, the_db_id, max_running)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@nosubmit_option()
@echo_option()
def check(level: LevelEnum, db: str, no_submit: bool, echo: bool, **kwargs: Any) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.check(level, the_db_id)


@cli.command("accept")
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@nosubmit_option()
@echo_option()
def accept(level: LevelEnum, db: str, no_submit: bool, echo: bool, **kwargs: Any) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.accept(level, the_db_id)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@nosubmit_option()
@echo_option()
def reject(level: LevelEnum, db: str, no_submit: bool, echo: bool, **kwargs: Any) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.reject(level, the_db_id)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@nosubmit_option()
@echo_option()
def supersede(level: LevelEnum, db: str, no_submit: bool, echo: bool, **kwargs: Any) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.supersede(level, the_db_id)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@status_option()
@db_option()
@nosubmit_option()
@echo_option()
def rollback(
    level: LevelEnum, status: StatusEnum, db: str, no_submit: bool, echo: bool, **kwargs: Any
) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.rollback(level, the_db_id, status)


@cli.command()
@level_option()
@fullname_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@status_option()
@db_option()
@echo_option()
@max_running_option()
def fake_run(level: LevelEnum, status: StatusEnum, db: str, echo: bool, **kwargs: Any) -> None:
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(level, **kwargs)
    iface.fake_run(level, the_db_id, status)


@cli.command()
@fullname_option()
@production_option()
@campaign_option()
@db_option()
@echo_option()
@nosubmit_option()
@max_running_option()
def daemon(db: str, echo: bool, no_submit: bool, max_running: int, **kwargs: Any) -> None:
    Handler.no_submit = no_submit
    iface = SQLAlchemyInterface(db, echo=echo)
    the_db_id = iface.get_db_id(LevelEnum.campaign, **kwargs)
    iface.daemon(the_db_id, max_running)
