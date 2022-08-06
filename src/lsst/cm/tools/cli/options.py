from enum import Enum
from functools import partial
from typing import Any, Type, TypeVar

import click

from ..core.utils import LevelEnum, StatusEnum, TableEnum

EnumType_co = TypeVar("EnumType_co", bound=Type[Enum], covariant=True)


class EnumChoice(click.Choice):
    """A version of click.Choice specialized for enum types"""

    def __init__(self, enum: EnumType_co, case_sensitive: bool = True) -> None:
        self._enum = enum
        super().__init__(list(enum.__members__.keys()), case_sensitive=case_sensitive)

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> EnumType_co:
        converted_str = super().convert(value, param, ctx)
        return self._enum.__members__[converted_str]


class MWOptionDecorator:
    """Wraps the click.option decorator to enable shared options to be declared
    and allows inspection of the shared option.
    """

    def __init__(self, *param_decls: Any, **kwargs: Any) -> None:
        self.partialOpt = partial(click.option, *param_decls, cls=partial(click.Option), **kwargs)
        opt = click.Option(param_decls, **kwargs)
        self._name = opt.name
        self._opts = opt.opts

    def name(self) -> str | None:
        """Get the name that will be passed to the command function for this
        option.
        """
        return self._name

    def opts(self) -> list[str]:
        """Get the flags that will be used for this option on the command
        line.
        """
        return self._opts

    @property
    def help(self) -> str:
        """Get the help text for this option. Returns an empty string if no
        help was defined.
        """
        return self.partialOpt.keywords.get("help", "")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.partialOpt(*args, **kwargs)


echo_option = MWOptionDecorator(
    "--echo",
    is_flag=True,
    help="Echo DB commands",
)

recurse_option = MWOptionDecorator(
    "--recurse",
    is_flag=True,
    help="Recurvisely execute command",
)

nosubmit_option = MWOptionDecorator(
    "--no-submit",
    is_flag=True,
    help="Don't submit jobs and scripts",
)

level_option = MWOptionDecorator(
    "--level",
    type=EnumChoice(LevelEnum),
    default="group",
    help="Which level to match.",
)

table_option = MWOptionDecorator(
    "--table",
    type=EnumChoice(TableEnum),
    default="workflow",
    help="Which database table to manipulate.",
)

status_option = MWOptionDecorator(
    "--status",
    type=EnumChoice(StatusEnum),
    default="completed",
    help="Status level to set.",
)

max_running_option = MWOptionDecorator(
    "--max-running",
    default=50,
    help="Maximum number of running workflows.",
)

butler_option = MWOptionDecorator(
    "--butler-repo",
    default="repo",
    help="URL for butler.",
)

prod_base_option = MWOptionDecorator(
    "--prod-base-url",
    default="archive",
    help="URL for production area base",
)

db_option = MWOptionDecorator(
    "--db",
    default="sqlite:///cm.db",
    help="URL for campaign management database.",
)

handler_option = MWOptionDecorator(
    "--handler",
    default="lsst.cm.tools.db.sqlalch_handler.SQLAlchemyHandler",
    help="Full import path to callback handler.",
)

data_query_option = MWOptionDecorator(
    "--data-query",
    help="Data query for entry",
)

config_option = MWOptionDecorator(
    "--config-yaml",
    type=click.Path(exists=True),
    help="Configuration Yaml.",
)

fullname_option = MWOptionDecorator(
    "--fullname",
    help="Full entry name.",
)

production_option = MWOptionDecorator(
    "--production-name",
    help="Production name.",
)

campaign_option = MWOptionDecorator(
    "--campaign-name",
    help="Campaign name.",
)

step_option = MWOptionDecorator(
    "--step-name",
    help="Step name.",
)

group_option = MWOptionDecorator(
    "--group-name",
    help="Group name.",
)

workflow_option = MWOptionDecorator(
    "--workflow-idx",
    type=int,
    help="Workflow index.",
)
