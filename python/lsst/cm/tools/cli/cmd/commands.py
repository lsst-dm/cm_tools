import sys
from typing import Any

import click
from lsst.cm.tools.cli.opt.options import (
    butler_option,
    campaign_option,
    config_option,
    db_option,
    echo_option,
    group_option,
    handler_option,
    level_option,
    max_running_option,
    prod_base_option,
    production_option,
    step_option,
    table_option,
    workflow_option,
)
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface

__all__ = [
    "cm_create",
    "cm_insert",
    "cm_print",
    "cm_print_table",
    "cm_print_tree",
    "cm_prepare",
    "cm_queue",
    "cm_launch",
    "cm_check",
    "cm_accept",
    "cm_reject",
    "cm_fake_run",
    "cm_daemon",
]


@click.command("create")
@db_option()
@echo_option()
def cm_create(**kwargs: Any) -> None:
    SQLAlchemyInterface(db_url=kwargs.get("db"), echo=kwargs.get("echo"), create=True)


@click.command("insert")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@butler_option()
@prod_base_option()
@workflow_option()
@handler_option()
@config_option()
@db_option()
@echo_option()
def cm_insert(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    config_yaml = all_args.pop("config_yaml")
    handler_class = all_args.pop("handler")
    if the_level != LevelEnum.production:
        assert config_yaml is not None
        assert handler_class is not None
        the_handler = Handler.get_handler(handler_class, config_yaml)
        the_db_id = iface.get_db_id(the_level, **all_args)
    else:
        the_db_id = None
        the_handler = None
    iface.insert(the_db_id, the_handler, **all_args)


@click.command("print_tree")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
def cm_print_tree(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.print_tree(sys.stdout, the_level, the_db_id)


@click.command("print")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
def cm_print(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.print_(sys.stdout, the_level, the_db_id)


@click.command("print_table")
@table_option()
@db_option()
@echo_option()
def cm_print_table(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    which_table = TableEnum[all_args.pop("table")]
    iface.print_table(sys.stdout, which_table)


@click.command("prepare")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
def cm_prepare(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    id_args = [
        "production_name",
        "campaign_name",
        "step_name",
        "group_name",
    ]
    for arg_ in id_args:
        all_args.pop(arg_)
    iface.prepare(the_level, the_db_id, **all_args)


@click.command("queue")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
def cm_queue(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.queue_workflows(the_level, the_db_id)


@click.command("launch")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
@max_running_option()
def cm_launch(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    max_running = all_args.pop("max_running")
    iface.launch_workflows(the_level, the_db_id, max_running)


@click.command("check")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
def cm_check(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)

    iface.check(the_level, the_db_id)


@click.command("accept")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
def cm_accept(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.accept(the_level, the_db_id)


@click.command("reject")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
def cm_reject(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.reject(the_level, the_db_id)


@click.command("fake_run")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@db_option()
@echo_option()
@max_running_option()
def cm_fake_run(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.fake_run(the_level, the_db_id, StatusEnum.completed)


@click.command("daemon")
@production_option()
@campaign_option()
@db_option()
@echo_option()
@max_running_option()
def cm_daemon(**kwargs: Any) -> None:
    all_args = kwargs.copy()
    max_running = all_args.pop("max_running")
    iface = SQLAlchemyInterface(db_url=all_args.pop("db"), echo=all_args.pop("echo"))
    the_db_id = iface.get_db_id(LevelEnum.campaign, **all_args)
    iface.daemon(the_db_id, max_running)
