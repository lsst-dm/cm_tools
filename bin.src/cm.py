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

import sys

import argparse

from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
from lsst.cm.tools.core.handler import Handler
from lsst.cm.tools.db.sqlalch_interface import SQLAlchemyInterface


if __name__ == '__main__':

    actions = [
        'create',
        'insert',
        'update',
        'print',
        'count',
        'print_table',
        'check',
        'prepare',
        'queue',
        'launch',
        'accept',
        'reject']
    parser = argparse.ArgumentParser(prog=sys.argv[0])

    parser.add_argument('--db', type=str, help='Database', default="sqlite:///cm.db")
    parser.add_argument('--action', type=str, help=f"One of {str(actions)}", default=None)
    parser.add_argument('--production_name', type=str, help="Production Name", default=None)
    parser.add_argument('--campaign_name', type=str, help="Campaign Name", default=None)
    parser.add_argument('--step_name', type=str, help="Step Name", default=None)
    parser.add_argument('--group_name', type=str, help="Group Name", default=None)
    parser.add_argument('--workflow_idx', type=int, help="Workflow Index", default=None)
    parser.add_argument('--level', type=int, help="Which table to use", default=None)
    parser.add_argument('--echo', action='store_true', default=False, help="Echo DB commands")
    parser.add_argument('--recurse', action='store_true', default=False, help="Turn on recursion on insert")
    parser.add_argument('--status', type=int, help="Status flag to set", default=None)
    parser.add_argument('--handler', type=str, help="Callback handler",
                        default='lsst.cm.tools.db.sqlalch_handler.SQLAlchemyHandler')
    parser.add_argument('--config_yaml', type=str, help="Configuration Yaml", default=None)

    args = parser.parse_args()

    if args.action not in actions:
        raise ValueError(f"action must be one of {str(actions)}")

    iface = SQLAlchemyInterface(args.db, echo=args.echo, create=args.action == 'create')

    if args.action == 'create':
        sys.exit(0)

    if args.level is None:
        raise ValueError("You must specify a level")

    all_args = args.__dict__.copy()
    the_level = LevelEnum(all_args.pop('level'))

    id_args = [
        'production_name',
        'campaign_name',
        'step_name',
        'group_name',
        'workflow_idx']

    the_db_id = iface.get_db_id(the_level, **all_args)

    if args.action == 'insert':
        config_yaml = all_args.pop('config_yaml')
        assert config_yaml is not None
        handler_class = all_args.pop('handler')
        recurse_value = all_args.pop('recurse')
        the_handler = Handler.get_handler(handler_class, config_yaml)
        iface.insert(the_level, the_db_id, the_handler, recurse_value, **all_args)
    elif args.action == 'count':
        print(iface.count(the_level, the_db_id))
    elif args.action == 'print':
        iface.print_(sys.stdout, the_level, the_db_id)
    elif args.action == 'check':
        recurse_value = all_args.pop('recurse')
        iface.check(the_level, the_db_id, recurse_value)
    elif args.action == 'prepare':
        recurse_value = all_args.pop('recurse')
        all_args.pop('handler')
        for arg_ in id_args:
            all_args.pop(arg_)
        iface.prepare(the_level, the_db_id, recurse_value, **all_args)
    elif args.action == 'queue':
        iface.queue_workflows(the_level, the_db_id)
    elif args.action == 'launch':
        iface.launch_workflows(the_level, the_db_id, 50)
    elif args.action == 'accept':
        iface.accept(the_level, the_db_id)
    elif args.action == 'reject':
        iface.reject(the_level, the_db_id)
    elif args.action == 'update':
        status_value = all_args.pop('status')
        for arg_ in id_args:
            all_args.pop(arg_)
        if status_value is not None:
            the_status = StatusEnum(all_args.pop('status'))
            iface.update(the_level, the_db_id, status=the_status, **all_args)
        else:
            iface.update(the_level, the_db_id, **all_args)
    elif args.action == 'print_table':
        iface.print_table(sys.stdout, the_level)
