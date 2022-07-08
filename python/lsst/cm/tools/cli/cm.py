#!/usr/bin/env python
# This file is part of cm_tools
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

import click
import lsst.cm.tools.cli.cmd.commands as cli_commands


class CMCLI(click.MultiCommand):
    def list_commands(self, ctx):
        return [cmd_.replace("cm_", "") for cmd_ in cli_commands.__all__]

    def get_command(self, ctx, cmd_name):
        return getattr(cli_commands, f"cm_{cmd_name}")


cli = CMCLI("campaign management tool")


def main():
    return cli()
