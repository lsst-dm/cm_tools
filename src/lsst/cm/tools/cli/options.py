from enum import Enum
from functools import partial, wraps
from typing import Any, Callable, Type, TypeVar, cast

import click
from click.decorators import _AnyCallable

from ..core.db_interface import DbInterface
from ..core.handler import Handler
from ..core.utils import LevelEnum, ScriptMethod, StatusEnum, TableEnum
from ..db.sqlalch_interface import SQLAlchemyInterface

__all__ = [
    "butler",
    "campaign",
    "config_yaml",
    "config_name",
    "config_block",
    "data_query",
    "diag_message",
    "dbi",
    "error_name",
    "error_yaml",
    "fmt",
    "fullname",
    "group",
    "idx",
    "level",
    "log_file",
    "lsst_version",
    "max_running",
    "n_iter",
    "panda_url",
    "username",
    "prod_base",
    "production",
    "purge",
    "rescuable",
    "review",
    "root_coll",
    "script",
    "script_method",
    "sleep_time",
    "status",
    "step",
    "summary",
    "table",
    "update_item",
    "verbose",
    "workflow",
    "yaml_output",
]


EnumType_co = TypeVar("EnumType_co", bound=Type[Enum], covariant=True)


class EnumChoice(click.Choice):
    """A version of click.Choice specialized for enum types"""

    def __init__(self, enum: EnumType_co, case_sensitive: bool = True) -> None:
        self._enum = enum
        super().__init__(list(enum.__members__.keys()), case_sensitive=case_sensitive)

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> EnumType_co:
        converted_str = super().convert(value, param, ctx)
        return self._enum.__members__[converted_str]


class PartialOption:
    """Wraps click.option with partial arguments for convenient reuse"""

    def __init__(self, *param_decls: Any, **kwargs: Any) -> None:
        self._partial = partial(click.option, *param_decls, cls=partial(click.Option), **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._partial(*args, **kwargs)


echo = PartialOption(
    "--echo",
    help="Echo DB commands",
    is_flag=True,
)

level = PartialOption(
    "--level",
    type=EnumChoice(LevelEnum),
    default=None,
    help="Which level to match.",
)

table = PartialOption(
    "--table",
    type=EnumChoice(TableEnum),
    default="workflow",
    help="Which database table to manipulate.",
)

fmt = PartialOption(
    "--fmt",
    default=None,
    help="Format for printing",
)

status = PartialOption(
    "--status",
    type=EnumChoice(StatusEnum),
    default="completed",
    help="Status level to set.",
)

script_method = PartialOption(
    "--script_method",
    type=EnumChoice(ScriptMethod),
    default="bash",
    envvar="CM_SCRIPT_METHOD",
    help="How to submit scripts.",
)

max_running = PartialOption(
    "--max-running",
    default=50,
    help="Maximum number of running workflows.",
)

sleep_time = PartialOption(
    "--sleep-time",
    default=60,
    help="sleep time between iterations, in seconds",
)

n_iter = PartialOption(
    "--n-iter",
    default=-1,
    help="number of interations to run daemon",
)

butler = PartialOption(
    "--butler-repo",
    default="repo",
    help="URL for butler.",
)

prod_base = PartialOption(
    "--prod-base-url",
    help="URL for production area base",
    default="archive",
    envvar="CM_PROD_URL",
    show_envvar=True,
    show_default=True,
)

db = PartialOption(
    "--db",
    help="URL for campaign management database.",
    default="sqlite:///cm.db",
    envvar="CM_DB",
    show_envvar=True,
    show_default=True,
)


data_query = PartialOption(
    "--data-query",
    help="Data query for entry",
)

config_yaml = PartialOption(
    "--config-yaml",
    type=click.Path(),
    help="Configuration Yaml.",
)

config_name = PartialOption(
    "--config-name",
    help="Configuration Name.",
)

config_block = PartialOption(
    "--config-block",
    help="Which block of configuration to use",
)

fullname = PartialOption(
    "--fullname",
    help="Full entry name.",
)

production = PartialOption(
    "--production-name",
    help="Production name.",
)

campaign = PartialOption(
    "--campaign-name",
    help="Campaign name.",
)

step = PartialOption(
    "--step-name",
    help="Step name.",
)

group = PartialOption(
    "--group-name",
    help="Group name.",
)

workflow = PartialOption(
    "--workflow-idx",
    type=int,
    help="Workflow index.",
)

script = PartialOption(
    "--script-name",
    help="Script name.",
)

idx = PartialOption(
    "--idx",
    type=int,
    default=0,
    help="Job or script index",
)

plugin_dir = PartialOption(
    "--plugin-dir",
    help="Additional directory to search for callback plug-ins.",
    envvar="CM_PLUGINS",
    show_envvar=True,
)

config_dir = PartialOption(
    "--config-dir",
    help="Directory root for entry configuration yaml files.",
    envvar="CM_CONFIGS",
    show_envvar=True,
)

lsst_version = PartialOption(
    "--lsst-version",
    help="Version of LSST software stack",
    envvar="CM_LSST_VERSION",
    show_envvar=True,
)

panda_url = PartialOption(
    "--panda-url",
    help="ReqID associated with the PanDA job.",
)

username = PartialOption(
    "--username",
    help="Username to be passed to workflow system.",
    default=None,
)

panda_code = PartialOption(
    "--panda-code",
    help="Error code generated by PanDA.",
)

purge = PartialOption(
    "--purge",
    help="Remove superseded collections from butler",
    is_flag=True,
)

review = PartialOption(
    "--review",
    help="Only print info about reviewable jobs",
    is_flag=True,
)

root_coll = PartialOption(
    "--root-coll",
    help="Root for output collection names.",
)

summary = PartialOption(
    "--summary",
    help="Print out a summary only",
    is_flag=True,
)

log_file = PartialOption(
    "--log-file",
    help="Log file",
    default=None,
)

rescuable = PartialOption(
    "--rescuable",
    default=False,
    help="mark as rescuable, instead of accepted",
    is_flag=True,
)

verbose = PartialOption(
    "--verbose",
    default=False,
    help="verbose flag",
    is_flag=True,
)

diag_message = PartialOption(
    "--diag-message",
    help="Diagnostic Error Message.",
)

error_name = PartialOption(
    "--error-name",
    help="Unique name for this ErrorType.",
)

error_yaml = PartialOption(
    "--error-yaml",
    type=click.Path(),
    help="Yaml file with errors to match",
)

update_item = PartialOption(
    "--update-item",
    type=(str, str),
    help="Item to update in Error.",
)

yaml_output = PartialOption(
    "--yaml-output",
    default=False,
    help="print output in yaml format",
    is_flag=True,
)


def dbi(create: bool = False) -> Callable[[_AnyCallable], _AnyCallable]:
    """Set up interface to underlying databases."""

    def decorator(f: _AnyCallable) -> _AnyCallable:
        @db(expose_value=False, callback=record_meta)
        @plugin_dir(expose_value=False, callback=record_meta)
        @config_dir(expose_value=False, callback=record_meta)
        @echo(expose_value=False, callback=record_meta)
        @click.option("--dbi", hidden=True, callback=make_dbi)
        @wraps(f)
        def wrapper(*args, **kwargs):  # type: ignore
            return f(*args, **kwargs)

        return cast(_AnyCallable, wrapper)

    def record_meta(ctx: click.Context, param: click.Parameter, value: Any) -> None:
        if value and param.name:
            ctx.meta[param.name] = value

    def make_dbi(ctx: click.Context, param: click.Parameter, value: Any) -> DbInterface:
        db_url = ctx.meta.get("db", param.get_default(ctx))
        Handler.plugin_dir = ctx.meta.get("plugin_dir", param.get_default(ctx))
        Handler.config_dir = ctx.meta.get("config_dir", param.get_default(ctx))
        return SQLAlchemyInterface(db_url, echo=ctx.meta.get("echo", param.get_default(ctx)), create=create)

    return decorator
