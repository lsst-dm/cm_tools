from __future__ import annotations

import types
from typing import TYPE_CHECKING, Any

from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name

from lsst.cm.tools.core.utils import InputType, OutputType, ScriptMethod, StatusEnum

from .utils import add_sys_path

if TYPE_CHECKING:  # pragma: no cover
    from lsst.cm.tools.core.db_interface import DbInterface, JobBase, ScriptBase
    from lsst.cm.tools.core.dbid import DbId
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
    handler_cache: dict[int, Handler] = {}
    script_method = ScriptMethod.bash

    config_block = ""

    plugin_dir: str | None = None
    config_dir: str | None = None

    def __init__(self, fragment_id: int, **kwargs: Any) -> None:
        self._fragment_id = fragment_id
        self._config = self.default_config.copy()
        self._config.update(**kwargs)

    @staticmethod
    def get_handler(
        fragment_id: int,
        class_name: str,
        **kwargs: Any,
    ) -> Handler:
        """Create and return a handler

        Parameters
        ----------
        fragment_id: int
            Id for the associated configuration fragment,

        class_name : str
            Name of the handler class requested

        kwargs : Any
            The configuration parameters

        Returns
        -------
        handler : Handler
            Requested handler

        Notes
        -----
        The handlers are cached by configuration fragment id.
        If a cached fragment is found that will be returned
        instead of producing a new one.
        """
        cached_handler = Handler.handler_cache.get(fragment_id)
        if cached_handler is None:
            with add_sys_path(Handler.plugin_dir):
                handler_class = doImport(class_name)
            if isinstance(handler_class, types.ModuleType):
                raise TypeError()
            cached_handler = handler_class(fragment_id, **kwargs)
            Handler.handler_cache[fragment_id] = cached_handler
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

    def get_config_var(self, varname: str, default: Any, **kwargs: Any) -> Any:
        """Utility function to get a configuration parameter value

        Parameters
        ----------
        varname : str
            Name of the parameter requested

        default : Any
            Default value of the parameter in question

        Keywords
        --------
        Can be used to override configuration value

        Returns
        -------
        par_value : Any
            Value of the requested parameter

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
            Name of the keyword requested

        Returns
        -------
        value : Any
            Value of the request keyword

        Raises
        ------
        KeyError :
            The requested keyword is not present
        """
        value = kwargs.get(key, "__FAIL__")
        if value == "__FAIL__":
            raise KeyError(f"Keyword {key} was not specified in {str(kwargs)}")
        return value

    def resolve_templated_string(self, template_str: str, **kwargs: Any) -> str:
        """Utility function to return a string from a template using kwargs

        Parameters
        ----------
        template_str : str
            Template to use

        Keywords
        --------
        These can be used in the formating

        Returns
        -------
        value : str
            Formatted string

        Raises
        ------
        KeyError :
            Formatting failed because of missing key
        """
        try:
            return template_str.format(**kwargs)
        except KeyError as msg:  # pragma: no cover
            raise KeyError(f"Failed to format {template_str} with {str(kwargs)}") from msg

    def resolve_templated_strings(self, **kwargs: Any) -> dict[str, Any]:
        """Utility function resolve a list of templated names

        This will format all the strings listed in `self.config.templates`

        Keywords
        --------
        These can be used in the formating

        Returns
        -------
        values : dict[str, Any]
            Formatted strings

        Raises
        ------
        KeyError :
            Formatting failed because of missing key
        """
        template_names = self.config.get("templates", {})
        return {key_: self.resolve_templated_string(val_, **kwargs) for key_, val_ in template_names.items()}


class ScriptHandlerBase(Handler):
    """Handler class for dealing with scripts

    By scripts we mean small shell scripts that
    are run to manipulate the collections in the
    processing.

    Some of these can take long enough that
    we want to run them independently of
    managing the database
    """

    config_block = "script"

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> ScriptBase:
        """Insert a new script

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        parent: Any
            Parent entry the new script is associated with

        Keywords
        --------
        These give the values we are inserting in the database

        Returns
        -------
        new_entry : ScriptBase
            New entry
        """
        raise NotImplementedError()

    def write_script_hook(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> None:
        """Write a script to maninpulate data / collections

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        parent: Any
            Parent entry the script is associated with

        script: ScriptBase
            Database entry for the script

        Keywords
        --------
        These can we used in writing the script
        """
        raise NotImplementedError()

    def fake_run_hook(
        self, dbi: DbInterface, script: ScriptBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        """Used for testing, falsely writes a stamp file that claims
        script is completed

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        script: ScriptBase
            Database entry for the script

        status: StatusEnum
            Status to set
        """
        raise NotImplementedError()

    def run(self, dbi: DbInterface, parent: Any, script: ScriptBase, **kwargs: Any) -> StatusEnum:
        """Run the script

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we updated

        parent : Any
            Entry associated to this script

        script: ScriptBase
            Database entry for the script

        kwargs: Any
            Passed to the function that writes the script

        Returns
        -------
        status: StatusEnum
            Status of the script
        """
        raise NotImplementedError()


class JobHandlerBase(Handler):
    """Handler class for dealing with jobs

    By jobs we mean processing jobs that run
    on batch systems
    """

    config_block = "job"

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> JobBase:
        """Insert a new script

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        parent: Any
            Parent entry the new script is associated with

        Keywords
        --------
        These give the values we are inserting in the database

        Returns
        -------
        new_entry : JobBase
            New entry
        """
        raise NotImplementedError()

    def write_job_hook(self, dbi: DbInterface, parent: Any, job: JobBase, **kwargs: Any) -> None:
        """Write a `job` script to run a workflow

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        parent: Any
            Parent entry the script is associated with

        job: JobBase
            Database entry for the job

        Keywords
        --------
        These can we used in writing the job
        """
        raise NotImplementedError()

    def fake_run_hook(
        self, dbi: DbInterface, job: JobBase, status: StatusEnum = StatusEnum.completed
    ) -> None:
        """Used for testing, falsely writes a log file that claims
        job is completed

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        job: JobBase
            Database entry for the script

        status: StatusEnum
            Status to set
        """
        raise NotImplementedError()

    def launch(self, dbi: DbInterface, job: JobBase) -> StatusEnum:
        """Launch the job

        This will submit the job to the workflow manager
        (e.g., PanDa or whatever)

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        job: JobBase
            Database entry for the job

        Returns
        -------
        status: StatusEnum
            Status of the job
        """
        raise NotImplementedError()


class EntryHandlerBase(Handler):
    """Handler class for dealing with Campaigns, Steps and Groups

    This collects the common functionality between them
    """

    default_config = dict(
        coll_in_template="prod/{fullname}_input",
        coll_out_template="prod/{fullname}_output",
        coll_validate_template="prod/{fullname}_validate",
    )

    coll_template_names = dict(
        coll_in="coll_in_template",
        coll_out="coll_out_template",
        coll_validate="coll_validate_template",
    )

    def insert(self, dbi: DbInterface, parent: Any, **kwargs: Any) -> CMTable:
        """Insert a new database entry

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        parent: Any
            Parent entry this entry is associated with

        Keywords
        --------
        These give the values we are inserting in the database

        Returns
        -------
        new_entry : CMTable
            New entry
        """
        raise NotImplementedError()

    def prepare(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        """Prepare an entry

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        entry: Any
            Entry in question

        Returns
        -------
        status : StatusEnum
            Status to set the entry to
        """
        raise NotImplementedError()

    def make_children(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        """Make and prepare any child entries

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        entry: Any
            Entry in question

        Returns
        -------
        status : StatusEnum
            Status to set the entry to
        """
        raise NotImplementedError()

    def check(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        """Check this entry and any children

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        entry: Any
            Entry in question

        Returns
        -------
        status : StatusEnum
            Status to set the entry to
        """
        raise NotImplementedError()

    def collect(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        """Run collection scripts for this entry and any children

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        entry: Any
            Entry in question

        Returns
        -------
        status : StatusEnum
            Status to set the entry to
        """
        raise NotImplementedError()

    def validate(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        """Validate this entry and any children

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        entry: Any
            Entry in question

        Returns
        -------
        status : StatusEnum
            Status to set the entry to
        """
        raise NotImplementedError()

    def accept(self, dbi: DbInterface, entry: Any, rescuable: bool = False) -> list[DbId]:
        """Accept this entry and any children

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        entry: Any
            Entry in question

        rescuable: bool
            Mark the entries as rescuable instead of accepted

        Returns
        -------
        db_ids : list[DbId]
            All the affected entries
        """
        raise NotImplementedError()

    def reject(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        """Reject this entry and any children

        Parameters
        ----------
        dbi: DbInterface
            Interface to the database we are using

        entry: Any
            Entry in question

        Returns
        -------
        db_ids : list[DbId]
            All the affected entries
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
        coll_name_map : dict[str, Any]
            Names of the input and output collections
            and input and output types
        """
        coll_name_map = self.resolve_templated_strings(
            **insert_fields,
            **kwargs,
        )
        input_type = InputType[self.get_config_var("input_type", "source", **kwargs)]
        output_type = OutputType[self.get_config_var("output_type", "run", **kwargs)]
        if input_type == InputType.source:
            coll_name_map.setdefault("coll_in", insert_fields.get("coll_source"))
        coll_name_map.update(
            input_type=input_type,
            output_type=output_type,
        )
        return coll_name_map

    def make_scripts(self, dbi: DbInterface, entry: Any) -> StatusEnum:
        """Called to set up scripts and jobs for an entry

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question

        Returns
        -------
        status : StatusEnum
            Status to set the entry to
        """
        raise NotImplementedError()

    def prepare_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        """Called to run scripts need to prepare an entry for execution

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question

        Returns
        -------
        scripts : list[ScriptBase]
            The newly made scripts
        """
        raise NotImplementedError()

    def collect_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        """Called when all the childern of a particular entry are finished

        Does any collection of results from children.

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question

        Returns
        -------
        scripts : list[ScriptBase]
            The newly made scripts
        """
        raise NotImplementedError()

    def validate_script_hook(self, dbi: DbInterface, entry: Any) -> list[ScriptBase]:
        """Called to validate an entry once the results have been collected

        This runs after collect() to make it easier to
        run validation on the collected results

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question

        Returns
        -------
        scripts : list[ScriptBase]
            The newly made scripts
        """
        raise NotImplementedError()

    def accept_hook(self, dbi: DbInterface, entry: Any) -> None:
        """Called when a particular entry is accepted

        Allows users to do any extra operations associated
        with accepting the entry

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question
        """
        raise NotImplementedError()

    def reject_hook(self, dbi: DbInterface, entry: Any) -> None:
        """Called when a particular entry is rejected

        Allows users to do any extra operations associated
        with rejecting the entry

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question
        """
        raise NotImplementedError()

    def supersede_hook(self, dbi: DbInterface, entry: Any) -> None:
        """Called when a particular entry is superseded

        Allows users to do any extra operations associated
        with superseding the entry

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question
        """
        raise NotImplementedError()

    def rollback(self, dbi: DbInterface, entry: Any, to_status: StatusEnum) -> list[DbId]:
        """Called to 'roll-back' a partiuclar entry

        Calling this function can result in scripts and child entries
        being marked as `superseded`, and be ignored by further processing.

        This will iterate from the current status to the
        requested status, marking as superseded any scripts and children
        that were produced when originally moving to the higher state.

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question

        to_status : StatusEnum
            Status we want to roll back to

        Returns
        -------
        db_ids : list[DbId]
            All the affected entries
        """
        raise NotImplementedError()

    def supersede(self, dbi: DbInterface, entry: Any) -> list[DbId]:
        """Called to mark an entry as superseded

        Superseded entries are ignored in futher processing, and
        do not prevent parent entries for continuing.

        Parameters
        ----------
        dbi : DbInterface
            Interface to the database we are using

        entry : Any
            Entry in question

        Returns
        -------
        db_ids : list[DbId]
            All the affected entries
        """
        raise NotImplementedError()
