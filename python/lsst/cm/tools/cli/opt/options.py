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

from functools import partial

import click  # type: ignore

from lsst.cm.tools.core.utils import LevelEnum


class MWOptionDecorator:
    """Wraps the click.option decorator to enable shared options to be declared
    and allows inspection of the shared option.
    """

    def __init__(self, *param_decls, **kwargs):
        self.partialOpt = partial(click.option, *param_decls, cls=partial(click.Option), **kwargs)
        opt = click.Option(param_decls, **kwargs)
        self._name = opt.name
        self._opts = opt.opts

    def name(self):
        """Get the name that will be passed to the command function for this
        option."""
        return self._name

    def opts(self):
        """Get the flags that will be used for this option on the command
        line."""
        return self._opts

    @property
    def help(self):
        """Get the help text for this option. Returns an empty string if no
        help was defined."""
        return self.partialOpt.keywords.get("help", "")

    def __call__(self, *args, **kwargs):
        return self.partialOpt(*args, **kwargs)


class OptionGroup:
    """Base class for an option group decorator. Requires the option group
    subclass to have a property called `decorator`."""

    def __call__(self, f):
        for decorator in reversed(self.decorators):
            f = decorator(f)
        return f


echo_option = MWOptionDecorator(
    "--echo",
    help="Echo DB commands",
    is_flag=True)

recurse_option = MWOptionDecorator(
    "--recurse",
    help="Recurvisely execute command",
    is_flag=True)

level_option = MWOptionDecorator(
    "--level",
    default="workflow",
    type=click.Choice(
        choices=list(LevelEnum.__members__.keys()),
        case_sensitive=True,
    ),
    help="Which database table to manipulate.")

max_running_option = MWOptionDecorator(
    "--max_running",
    default=50,
    help="Maximum number of running workflows.")

db_option = MWOptionDecorator(
    '--db',
    default='sqlite:///cm.db',
    help="URL for campaign management database.")

handler_option = MWOptionDecorator(
    '--handler',
    default='lsst.cm.tools.db.sqlalch_handler.SQLAlchemyHandler',
    help="Full import path to callback handler.")

config_option = MWOptionDecorator(
    '--config_yaml',
    type=click.Path(exists=True),
    help="Configuration Yaml.")

production_option = MWOptionDecorator(
    '--production_name',
    help="Production name.")

campaign_option = MWOptionDecorator(
    '--campaign_name',
    help="Campaign name.")

step_option = MWOptionDecorator(
    '--step_name',
    help="Step name.")

group_option = MWOptionDecorator(
    '--group_name',
    help="Group name.")

workflow_option = MWOptionDecorator(
    '--workflow_name',
    type=int,
    help="Workflow name.")


class IdOptions(OptionGroup):
    def __init__(self):
        self.decorators = []
        self.decorators.append(production_option)
        self.decorators.append(campaign_option)
        self.decorators.append(step_option)
        self.decorators.append(group_option)
        self.decorators.append(workflow_option)


id_options = IdOptions()
