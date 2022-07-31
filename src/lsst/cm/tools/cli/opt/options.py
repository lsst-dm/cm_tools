from functools import partial
from typing import Any, List, Optional, Reversible

import click

from lsst.cm.tools.core.utils import LevelEnum, StatusEnum, TableEnum


class MWOptionDecorator:
    """Wraps the click.option decorator to enable shared options to be declared
    and allows inspection of the shared option.
    """

    def __init__(self, *param_decls: Any, **kwargs: Any) -> None:
        self.partialOpt = partial(click.option, *param_decls, cls=partial(click.Option), **kwargs)
        opt = click.Option(param_decls, **kwargs)
        self._name = opt.name
        self._opts = opt.opts

    def name(self) -> Optional[str]:
        """Get the name that will be passed to the command function for this
        option."""
        return self._name

    def opts(self) -> List[str]:
        """Get the flags that will be used for this option on the command
        line."""
        return self._opts

    @property
    def help(self) -> str:
        """Get the help text for this option. Returns an empty string if no
        help was defined."""
        return self.partialOpt.keywords.get("help", "")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.partialOpt(*args, **kwargs)


class OptionGroup:
    """Base class for an option group decorator. Requires the option group
    subclass to have a property called `decorator`."""

    decorators: Reversible

    def __call__(self, f: Any) -> Any:
        for decorator in reversed(self.decorators):
            f = decorator(f)
        return f


echo_option = MWOptionDecorator("--echo", help="Echo DB commands", is_flag=True)

recurse_option = MWOptionDecorator("--recurse", help="Recurvisely execute command", is_flag=True)

nosubmit_option = MWOptionDecorator("--no-submit", help="Don't submit jobs and scripts", is_flag=True)

level_option = MWOptionDecorator(
    "--level",
    default="group",
    type=click.Choice(
        choices=list(LevelEnum.__members__.keys()),
        case_sensitive=True,
    ),
    help="Which level to match.",
)

table_option = MWOptionDecorator(
    "--table",
    default="workflow",
    type=click.Choice(
        choices=list(TableEnum.__members__.keys()),
        case_sensitive=True,
    ),
    help="Which database table to manipulate.",
)

status_option = MWOptionDecorator(
    "--status",
    default="completed",
    type=click.Choice(
        choices=list(StatusEnum.__members__.keys()),
        case_sensitive=True,
    ),
    help="Status level to set.",
)


max_running_option = MWOptionDecorator(
    "--max_running", default=50, help="Maximum number of running workflows."
)

butler_option = MWOptionDecorator("--butler_repo", default="repo", help="URL for butler.")

prod_base_option = MWOptionDecorator(
    "--prod_base_url", default="archive", help="URL for production area base"
)

db_option = MWOptionDecorator("--db", default="sqlite:///cm.db", help="URL for campaign management database.")

handler_option = MWOptionDecorator(
    "--handler",
    default="lsst.cm.tools.db.sqlalch_handler.SQLAlchemyHandler",
    help="Full import path to callback handler.",
)

data_query_option = MWOptionDecorator("--data_query", help="Data query for entry")

config_option = MWOptionDecorator("--config_yaml", type=click.Path(exists=True), help="Configuration Yaml.")

fullname_option = MWOptionDecorator("--fullname", help="Full entry name.")

production_option = MWOptionDecorator("--production_name", help="Production name.")

campaign_option = MWOptionDecorator("--campaign_name", help="Campaign name.")

step_option = MWOptionDecorator("--step_name", help="Step name.")

group_option = MWOptionDecorator("--group_name", help="Group name.")

workflow_option = MWOptionDecorator("--workflow_idx", type=int, help="Workflow index.")


class IdOptions(OptionGroup):
    def __init__(self) -> None:
        self.decorators = []
        self.decorators.append(production_option)
        self.decorators.append(campaign_option)
        self.decorators.append(step_option)
        self.decorators.append(group_option)
        self.decorators.append(workflow_option)


id_options = IdOptions()
