# This file is part of cm_tools.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import sys

import click  # type: ignore
from lsst.cm.tools.cli.opt.options import (
    campaign_option,
    config_option,
    db_option,
    echo_option,
    group_option,
    handler_option,
    level_option,
    max_running_option,
    production_option,
    recurse_option,
    step_option,
    workflow_option,
)
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface

__all__ = [
    "cm_create",
    "cm_insert",
    "cm_print",
    "cm_print_table",
    "cm_count",
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
def cm_create(**kwargs):
    SQLAlchemyInterface(db=kwargs.get("db"), echo=kwargs.get("echo"), create=True)


@click.command("insert")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@handler_option(required=True)
@config_option()
@db_option()
@echo_option()
@recurse_option()
def cm_insert(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    config_yaml = all_args.pop("config_yaml")
    assert config_yaml is not None
    handler_class = all_args.pop("handler")
    recurse_value = all_args.pop("recurse")
    the_handler = Handler.get_handler(handler_class, config_yaml)
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.insert(the_level, the_db_id, the_handler, recurse_value, **all_args)


@click.command("print")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
def cm_print(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.print_(sys.stdout, the_level, the_db_id)


@click.command("print_table")
@level_option()
@db_option()
@echo_option()
def cm_print_table(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    iface.print_table(sys.stdout, the_level)


@click.command("count")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
def cm_count(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    print(iface.count(the_level, the_db_id))


@click.command("prepare")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
@recurse_option()
def cm_prepare(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    recurse_value = all_args.pop("recurse")
    id_args = [
        "production_name",
        "campaign_name",
        "step_name",
        "group_name",
        "workflow_name",
    ]
    for arg_ in id_args:
        all_args.pop(arg_)
    iface.prepare(the_level, the_db_id, recurse_value, **all_args)


@click.command("queue")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
@recurse_option()
def cm_queue(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.queue_workflows(the_level, the_db_id)


@click.command("launch")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
@recurse_option()
@max_running_option()
def cm_launch(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
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
@workflow_option()
@db_option()
@echo_option()
@recurse_option()
def cm_check(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    recurse_value = all_args.pop("recurse")
    iface.check(the_level, the_db_id, recurse_value)


@click.command("accept")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
@recurse_option()
def cm_accept(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    recurse_value = all_args.pop("recurse")
    iface.accept(the_level, the_db_id, recurse_value)


@click.command("reject")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
def cm_reject(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.reject(the_level, the_db_id)


@click.command("fake_run")
@level_option()
@production_option()
@campaign_option()
@step_option()
@group_option()
@workflow_option()
@db_option()
@echo_option()
@recurse_option()
@max_running_option()
def cm_fake_run(**kwargs):
    all_args = kwargs.copy()
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_level = LevelEnum[all_args.pop("level")]
    the_db_id = iface.get_db_id(the_level, **all_args)
    iface.fake_run(the_db_id, StatusEnum.completed)


@click.command("daemon")
@production_option()
@campaign_option()
@db_option()
@echo_option()
@max_running_option()
def cm_daemon(**kwargs):
    all_args = kwargs.copy()
    max_running = all_args.pop("max_running")
    iface = SQLAlchemyInterface(db=all_args.pop("db"), echo=all_args.pop("echo"))
    the_db_id = iface.get_db_id(LevelEnum.campaign, **all_args)
    iface.daemon(the_db_id, max_running)
