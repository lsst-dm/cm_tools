from typing import Any, List, Optional

import click

import lsst.cm.tools.cli.cmd.commands as cli_commands


class CMCLI(click.MultiCommand):
    def list_commands(self, ctx: click.Context) -> List[str]:
        return [cmd_.replace("cm_", "") for cmd_ in cli_commands.__all__]

    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        return getattr(cli_commands, f"cm_{cmd_name}")


cli = CMCLI("campaign management tool")


def main() -> Any:
    return cli()
