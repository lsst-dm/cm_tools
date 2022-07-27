from __future__ import annotations

import types
from typing import TYPE_CHECKING, Any, Iterable, Optional

import yaml
from lsst.cm.tools.core.dbid import DbId
from lsst.cm.tools.core.utils import InputType, StatusEnum
from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name

if TYPE_CHECKING:  # pragma: no cover
    from lsst.cm.tools.core.db_interface import DbInterface, ScriptBase, WorkflowBase
    from lsst.cm.tools.db.common import CMTable


class Handler:
    """Base class to handle callbacks generated by particular
    database actions.

    Each entry in the database will have an associated
    Handler and configuration file, which will be called
    where particular database actions are taken.
    """

    fullname_template = ""

    default_config: dict[str, Any] = {}

    handler_cache: dict[str, Handler] = {}

    def __init__(self) -> None:
        self._config_url: Optional[str] = None
        self._config: dict[str, Any] = {}

    @property
    def config_url(self) -> Optional[str]:
        """Return the url of the file with the handler configuration"""
        return self._config_url

    @staticmethod
    def get_handler(class_name: str, config_url: str) -> Handler:
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
            if isinstance(handler_class, types.ModuleType):
                raise TypeError()
            cached_handler = handler_class()
            Handler.handler_cache[class_name] = cached_handler
        cached_handler.update_config(config_url)
        return cached_handler

    @classmethod
    def get_fullname(cls, **kwargs: Any) -> str:
        """Get a unique name for a particular database entry"""
        return cls.fullname_template.format(**kwargs)

    @property
    def config(self) -> dict[str, Any]:
        """Return the handler's configuration"""
        return self._config

    def get_handler_class_name(self) -> str:
        """Return this class's full name"""
        return get_full_type_name(self)

    def _read_config(self, config_url: str) -> None:
        """Utility function to read and cache a configuration from a URL"""
        self._config_url = config_url
        with open(self._config_url, "rt", encoding="utf-8") as config_file:
            self._config = yaml.safe_load(config_file)

    def update_config(self, config_url: str) -> None:
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

    def get_config_var(self, varname: str, default: Any, **kwargs: Any) -> Any:
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

    @staticmethod
    def get_kwarg_value(key: str, **kwargs: Any) -> Any:
        """Utility function to get a keyword value

        Provides a more useful error message if the keyword is not present

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
        value = kwargs.get(key, "__FAIL__")
        if value == "__FAIL__":
            raise KeyError(f"Keyword {key} was not specified in {str(kwargs)}")
        return value

    def resolve_templated_string(self, template_name: str, **kwargs: Any) -> str:
        """Utility function to return a string from a template using kwargs

        Parameters
        ----------
        template_name : str
            The name of the template requested, must be in self.config

        Keywords
        --------
        These can be used in the formating

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
        format_vars = self.config.copy()
        format_vars.update(**kwargs)
        try:
            return template_string.format(**format_vars)
        except KeyError as msg:
            raise KeyError(f"Failed to format {template_string} with {str(format_vars)}") from msg

    def resolve_templated_strings(self, template_names: dict[str, str], **kwargs: Any) -> dict[str, Any]:
        """Utility function resolve a list of templated names

        Parameters
        ----------
        template_names : dict[str, str]
            Keys are the output keys, values are he names
            of the template requested, which must be in self.config

        Keywords
        --------
        These can be used in the formating

        Returns
        -------
        values : dict[str, Any]
            The formatted strings

        Raises
        ------
        KeyError :
            The formatting failed
        """
        return {key_: self.resolve_templated_string(val_, **kwargs) for key_, val_ in template_names.items()}


class ScriptHandlerBase(Handler):
    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> ScriptBase:
        """Insert a new script

        Parameters
        ----------

        Returns
        -------
        new_entry : CMTableBase
            The new entry
        """
        raise NotImplementedError()


class WorkflowHandlerBase(Handler):
    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> WorkflowBase:
        """Insert a new database entry

        Parameters
        ----------

        Keywords
        --------

        Returns
        -------
        new_entry : WorkflowBase
            The new entry
        """
        raise NotImplementedError()

    def workflow_script_hook(self, dbi: DbInterface, entry: Any, **kwargs: Any) -> WorkflowBase:
        """Write the script to run a workflow

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        Returns
        -------
        workflow : WorkflowBase
            The newly inserted workflow
        """
        raise NotImplementedError()

    def fake_run_hook(self, dbi: DbInterface, entry: Any, status: StatusEnum = StatusEnum.completed) -> None:
        """Used for testing

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        """
        raise NotImplementedError()


class EntryHandlerBase(Handler):

    default_config = dict(
        coll_in_template="prod/{fullname}_input",
        coll_out_template="prod/{fullname}_output",
    )

    coll_template_names = dict(
        coll_in="coll_in_template",
        coll_out="coll_out_template",
    )

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> CMTable:
        """Insert a new database entry

        Parameters
        ----------

        Keywords
        --------

        Returns
        -------
        new_entry : CMTableBase
            The new entry
        """
        raise NotImplementedError()

    def prepare(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        """Prepare this entry and any children

        Parameters
        ----------


        Returns
        -------
        db_id_list : list[DbId]
            All of the affected entries
        """
        raise NotImplementedError()

    def check(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        """Check this entry and any children

        Parameters
        ----------


        Returns
        -------
        db_id_list : list[DbId]
            All of the affected entries
        """
        raise NotImplementedError()

    def validate(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        """Validate this entry and any children

        Parameters
        ----------


        Returns
        -------
        db_id_list : list[DbId]
            All of the affected entries
        """
        raise NotImplementedError()

    def accept(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        """Accept this entry and any children

        Parameters
        ----------


        Returns
        -------
        db_id_list : list[DbId]
            All of the affected entries
        """
        raise NotImplementedError()

    def reject(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        """Reject this entry and any children

        Parameters
        ----------


        Returns
        -------
        db_id_list : list[DbId]
            All of the affected entries
        """
        raise NotImplementedError()

    def coll_names(self, insert_fields: dict, **kwargs: Any) -> dict[str, str]:
        """Called to get the name of the input and output collections

        Parameters
        ----------
        insert_fields : dict
            Fields used for most recent database insertion,
            can be used in formatting

        Returns
        -------
        coll_names : dict[str, str]
            Names of the input and output collections
        """
        default_coll_names = self.resolve_templated_strings(
            self.coll_template_names,
            **insert_fields,
            **kwargs,
        )
        if insert_fields.get("input_type") == InputType.source:
            default_coll_names["coll_in"] = insert_fields["coll_source"]
        return default_coll_names

    def prepare_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        """Called to set up scripts need to prepare an entry for execution

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        data : ???
            Current data for the entry we are preparing

        Returns
        -------
        scripts : list[ScriptBase]
            The newly made scripts
        """
        raise NotImplementedError()

    def collect_script_hook(self, dbi: DbInterface, itr: Iterable, entry: Any) -> list[ScriptBase]:
        """Called when all the childern of a particular entry are finished

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        itr : Iterable
            Iterator over children of the entry we are updating

        data : ???
            The data associated to this entry
        """
        raise NotImplementedError()

    def accept_hook(self, dbi: DbInterface, itr: Iterable, entry: Any) -> None:
        """Called when a particular entry is accepted

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        data : ???
            The data associated to this entry
        """
        raise NotImplementedError()

    def reject_hook(self, dbi: DbInterface, entry: Any) -> None:
        """Called when a particular entry is rejected

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        data : ???
            The data associated to this entry
        """
        raise NotImplementedError()
