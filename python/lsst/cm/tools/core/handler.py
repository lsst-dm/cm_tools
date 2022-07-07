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

from __future__ import annotations  # Needed for class member returning class

import yaml

from typing import Any, Iterable, TYPE_CHECKING

from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name

from lsst.cm.tools.core.utils import LevelEnum, StatusEnum
if TYPE_CHECKING:  # pragma: no cover
    from lsst.cm.tools.core.db_interface import DbId, DbInterface


class Handler:
    """Base class to handle callbacks generated by particular
    database actions.

    Each entry in the database will have an associated
    Handler and configuration file, which will be called
    where particular database actions are taken.
    """

    default_config: dict[str, Any] = {}

    handler_cache: dict[str, Handler] = {}

    def __init__(self):
        self._config_url = None
        self._config = {}

    @staticmethod
    def get_handler(
            class_name: str,
            config_url: str) -> Handler:
        """Create and return a handler

        Parameters
        ----------
        class_name : str
            Name of the handler class requested

        config_url : str
            URL to the configuration file for the handler

        Returns
        -------
        handler : Handler
            The requested handler

        Notes
        -----
        There are two layers of caching here.
        1.  A `dict` of Handler objects, keyed by class name
        2.  Each handler caches the config_url to avoid re-reading if
        it has not changed.
        """
        cached_handler = Handler.handler_cache.get(class_name)
        if cached_handler is None:
            handler_class = doImport(class_name)
            cached_handler = handler_class()  # type: ignore
            Handler.handler_cache[class_name] = cached_handler
        cached_handler.update_config(config_url)
        return cached_handler

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    def update_config(
            self,
            config_url: str) -> None:
        """Update this handler's configuration by reading a
        yaml configuration file

        Parameters
        ----------
        config_url : str
            The URL of the configuration file
        """
        assert config_url is not None
        if config_url == self._config_url:
            return
        self._read_config(config_url)

    def get_handler_class_name(self) -> str:
        """Return this classes full name"""
        return get_full_type_name(self)

    def get_insert_fields(
            self,
            level: LevelEnum,
            dbi: DbInterface,
            parent_db_id: DbId,
            **kwargs) -> dict[str, Any]:
        """Get the fields needed to insert an entry into the database

        Parameters
        ----------
        level : LevelEnum
            Specify which table we are inserting into

        dbi : DbInterface
            Interface to the database we are inserting into

        parent_db_id : DbId
            Database ID for the parent entry to the one we are inserting

        Keywords
        --------
        Keywords can be used by sub-classes to compute insert field values

        Returns
        -------
        insert_fields : dict[str, Any]
            The fields and value to insert
        """
        raise NotImplementedError()  # pragma: no cover

    def post_insert_hook(
            self,
            level: LevelEnum,
            dbi: DbInterface,
            insert_fields: dict[str, Any],
            recurse: bool = False,
            **kwargs) -> None:
        """Called after inserting an entry into the database

        Can be used to insert additional entries, i.e., children of this entry

        Can also be used to do any actions associated to inserting this entry,
        i.e., writing configuration files

        Parameters
        ----------
        level : LevelEnum
            Specify which table we inserted into

        dbi : DbInterface
            Interface to the database we intserted into

        insert_fields : dict
            List of fields and values that were inserted

        Keywords
        --------
        Keywords can be used by sub-classes
        """
        raise NotImplementedError()  # pragma: no cover

    def get_update_fields(
            self,
            level: LevelEnum,
            dbi: DbInterface,
            data,
            itr: Iterable,
            **kwargs) -> dict[str, Any]:
        """Get the fields needed to update an entry into the database

        Parameters
        ----------
        level : LevelEnum
            Specify which table we are updating

        dbi : DbInterface
            Interface to the database we are updating

        data : ???
            Current data for the entry we are updating

        itr : Iterable
            Iterator over childer of the entry we are updating

        Keywords
        --------
        Keywords can be used by sub-classes to compute field values

        Returns
        -------
        update_fields : dict[str, Any]
            The fields and value to update
        """
        raise NotImplementedError()  # pragma: no cover

    def prepare_hook(
            self,
            level: LevelEnum,
            dbi: DbInterface,
            db_id: DbId,
            data,
            recurse: bool = True,
            **kwargs) -> None:
        """Called when preparing a database entry for execution

        Can be used to prepare additional entries, for example,
        the children of this entry.

        Can also be used to do any actions associated to preparing this entry,
        e.g., making TAGGED Butler collections

        Parameters
        ----------
        level : LevelEnum
            Specify which table we updated

        dbi : DbInterface
            Interface to the database we updated

        db_id : DbId
            Database ID for this entry

        data : ???
            Current data for the entry we are preparing

        Keywords
        --------
        Keywords can be used by sub-classes
        """
        raise NotImplementedError()  # pragma: no cover

    def launch_workflow(
            self,
            dbi: DbInterface,
            db_id: DbId,
            data):
        """Launch a particular workflow

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        db_id : DbId
            Database ID for this entry

        data : ???
            The data associated to this entry
        """
        raise NotImplementedError()  # pragma: no cover

    def check_workflow_status_hook(
            self,
            dbi: DbInterface,
            db_id: DbId,
            data) -> dict[str, Any]:
        """Check the status of a particular workflow

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        db_id : DbId
            Database ID for this entry

        data : ???
            The data associated to this entry

        Returns
        -------
        update_fields : dict[str, Any]
            Used to update the status of the workflow in question.
        """
        raise NotImplementedError()  # pragma: no cover

    def accept_hook(
            self,
            level: LevelEnum,
            dbi: DbInterface,
            db_id: DbId,
            itr: Iterable,
            data) -> None:
        """Called when a particular entry is accepted

        Parameters
        ----------
        level : LevelEnum
            Specify which table we updated

        dbi : DbInterface
            Interface to the database we updated

        db_id : DbId
            Database ID for this entry

        data : ???
            The data associated to this entry
        """
        raise NotImplementedError()  # pragma: no cover

    def reject_hook(
            self,
            level: LevelEnum,
            dbi: DbInterface,
            db_id: DbId,
            data) -> None:
        """Called when a particular entry is rejected

        Parameters
        ----------
        level : LevelEnum
            Specify which table we updated

        dbi : DbInterface
            Interface to the database we updated

        db_id : DbId
            Database ID for this entry

        data : ???
            The data associated to this entry
        """
        raise NotImplementedError()  # pragma: no cover

    def fake_run_hook(
            self,
            dbi: DbInterface,
            db_id: DbId,
            data,
            status: StatusEnum = StatusEnum.completed) -> None:
        """Pretend to run workflows, this is for testing

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        db_id : DbId
            Specifies the entries we are running

        data :  ???
            The data associated to this entry

        status: StatusEnum
            Status value to set
        """
        raise NotImplementedError()  # pragma: no cover

    def _read_config(
            self,
            config_url: str) -> None:
        """Utility function to read and cache a configuration from a URL"""
        self._config_url = config_url
        with open(self._config_url, 'rt', encoding='utf-8') as config_file:
            self._config = yaml.safe_load(config_file)

    def _get_config_var(
            self,
            varname: str,
            default: Any,
            **kwargs) -> Any:
        """Utility function to get a configuration parameter value

        Parameters
        ----------
        varname : str
            The name of the parameter requested

        default : Any
            The default value of the parameter in question

        Keywords
        --------
        Can be used to override configuration value

        Returns
        -------
        par_value : Any
            The value of the requested parameter

        Notes
        -----
        The resolution order is:
            1. Return the value from the kwargs if it is present there
            2. Return the value from the config if it is present there
            3. Return the provided default value
        """
        return kwargs.get(varname, self.config.get(varname, default))

    @classmethod
    def _get_kwarg_value(
            cls,
            key: str,
            **kwargs) -> Any:
        """Utility function to get a keyword value

        Provides a more usefull error message if the keyword is not present

        Parameters
        ----------
        key : str
            The name of the keyword requested

        Returns
        -------
        value : Any
            The value of the request keyword

        Raises
        ------
        KeyError :
            The requested keyword is not present
        """
        value = kwargs.get(key, '__FAIL__')
        if value == '__FAIL__':  # pragma: no cover
            raise KeyError(f'Keyword {key} was not specified in {str(kwargs)}')
        return value

    def _resolve_templated_string(
            self,
            template_name: str,
            insert_fields: dict,
            **kwargs) -> str:
        """Utility function to return a string from a template using kwargs

        Parameters
        ----------
        template_name : str
            The name of the template requested, must be in self.config

        insert_fields : dict
            Fields used for most recent database insertion,
            can be used in formatting

        Keywords
        --------
        Keywords are also used in formating

        Returns
        -------
        value : str
            The formatted string

        Raises
        ------
        KeyError :
            The formatting failed
        """
        template_string = self.config.get(template_name, self.default_config.get(template_name))
        format_vars = kwargs.copy()
        format_vars.update(**insert_fields)
        try:
            return template_string.format(**format_vars)
        except KeyError as msg:  # pragma: no cover
            raise KeyError(f"Failed to format {template_string} with {str(kwargs)}") from msg
