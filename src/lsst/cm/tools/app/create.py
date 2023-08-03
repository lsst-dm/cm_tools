import os

import yaml
from flask import Flask, redirect, render_template, request, url_for

from ..core.db_interface import DbInterface
from ..core.utils import LevelEnum, StatusEnum, TableEnum
from ..db.error_table import ErrorAction, ErrorFlavor

SECRET_KEY = """
I was the shadow of the waxwing slain
By the false azure in the windowpane;
I was that smudge of ashen fluff–and I
Lived on, flew on, in the reflected sky.
"""

MAX_RUNNING = 50

TABLE_ACTIONS = {
    LevelEnum.production: [
        "insert_campaign",
    ],
    LevelEnum.campaign: [
        "insert_step",
        "check",
        "accept",
        "reject",
        "queue",
        "launch",
        "fake_run",
        "supersede",
        "add_script",
    ],
    LevelEnum.step: [
        "insert_group",
        "check",
        "accept",
        "reject",
        "queue",
        "launch",
        "fake_run",
        "supersede",
        "add_script",
    ],
    LevelEnum.group: [
        "insert_rescue",
        "check",
        "accept",
        "reject",
        "queue",
        "launch",
        "fake_run",
        "supersede",
        "add_script",
    ],
    LevelEnum.workflow: [
        "insert_rescue",
        "check",
        "accept",
        "reject",
        "queue",
        "launch",
        "fake_run",
        "supersede",
        "add_script",
    ],
}


attribute_dict = {
    LevelEnum.production: [],
    LevelEnum.campaign: [
        ("id", False),
        ("config_id", False),
        ("frag_id", False),
        ("fullname", False),
        ("data_query", True),
        ("bps_yaml_template", True),
        ("bps_script_template", True),
        ("coll_source", True),
        ("coll_in", True),
        ("coll_out", True),
        ("coll_validate", True),
        ("coll_ancil", True),
        ("butler_repo", True),
        ("lsst_version", True),
        ("lsst_custom_setup", True),
        ("root_coll", True),
        ("prod_base_url", True),
    ],
    LevelEnum.step: [
        ("id", False),
        ("config_id", False),
        ("frag_id", False),
        ("fullname", False),
        ("data_query", True),
        ("bps_yaml_template", True),
        ("bps_script_template", True),
        ("coll_source", True),
        ("coll_in", True),
        ("coll_out", True),
        ("coll_validate", True),
        ("lsst_version", True),
        ("lsst_custom_setup", True),
    ],
    LevelEnum.group: [
        ("id", False),
        ("config_id", False),
        ("frag_id", False),
        ("fullname", False),
        ("data_query", True),
        ("bps_yaml_template", True),
        ("bps_script_template", True),
        ("coll_source", True),
        ("coll_in", True),
        ("coll_out", True),
        ("coll_validate", True),
        ("lsst_version", True),
        ("lsst_custom_setup", True),
    ],
    LevelEnum.workflow: [
        ("id", False),
        ("config_id", False),
        ("frag_id", False),
        ("fullname", False),
        ("data_query", True),
        ("bps_yaml_template", True),
        ("bps_script_template", True),
        ("pipeline_yaml", True),
        ("coll_in", True),
        ("coll_out", True),
        ("coll_validate", True),
        ("lsst_version", True),
        ("lsst_custom_setup", True),
    ],
}


class CMFlask(Flask):
    def __init__(self, appname, dbi: DbInterface):
        Flask.__init__(self, appname)
        self._dbi = dbi

    @property
    def dbi(self) -> DbInterface:
        return self._dbi

    def set_dbi(self, dbi: DbInterface) -> DbInterface:
        self._dbi = dbi
        return self._dbi

SECRET_KEY = """
I was the shadow of the waxwing slain
By the false azure in the windowpane;
I was that smudge of ashen fluff–and I
Lived on, flew on, in the reflected sky.
"""

def create(dbi: DbInterface) -> CMFlask:
    app = CMFlask("lsst.cm.tools.app", dbi)
    app.config["SECRET_KEY"] = SECRET_KEY

    @app.template_global("get_attribute")
    def get_attribute(element, attr):
        return getattr(element, attr)

    @app.template_global("count_errors")
    def count_errors(element):
        n = 0
        for job_ in element.jobs_:
            n += len(job_.errors_)
        return n

    @app.template_global("count_errors_in_job")
    def count_errors_in_job(job):
        return len(job.errors_)

    @app.template_global("count_jobs")
    def count_jobs(jobs):
        n_tot = 0
        n_accepted = 0
        n_rescuable = 0
        n_failed = 0
        n_running = 0
        n_review = 0
        n_wait = 0
        for job_ in jobs:
            if job_.superseded:
                continue
            n_tot += 1
            if job_.status.is_bad:
                n_failed += 1
            elif job_.status.is_not_yet_processing:
                n_wait += 1
            elif job_.status.is_now_processing:
                n_running += 1
            elif job_.status.is_reviewable:
                n_review += 1
            elif job_.status.is_accepted:
                n_accepted += 1
            elif job_.status.is_rescuable:
                n_rescuable += 1
        return (n_tot, n_wait, n_running, n_review, n_accepted, n_rescuable, n_failed)

    @app.template_global("child_status_tuple")
    def child_status_tuple(element):
        n_tot = 0
        n_accepted = 0
        n_rescuable = 0
        n_failed = 0
        n_running = 0
        n_review = 0
        n_wait = 0
        for child_ in element.children():
            if child_.superseded:
                continue
            n_tot += 1
            if child_.status.is_bad:
                n_failed += 1
            elif child_.status.is_not_yet_processing:
                n_wait += 1
            elif child_.status.is_now_processing:
                n_running += 1
            elif child_.status.is_reviewable:
                n_review += 1
            elif child_.status.is_accepted:
                n_accepted += 1
            elif child_.status.is_rescuable:
                n_rescuable += 1
        return (n_tot, n_wait, n_running, n_review, n_accepted, n_rescuable, n_failed)

    @app.template_global("count_scripts")
    def count_scripts(scripts):
        n_tot = 0
        n_accepted = 0
        n_failed = 0
        n_running = 0
        n_wait = 0
        for script_ in scripts:
            n_tot += 1
            if script_.status.is_bad_script:
                n_failed += 1
            elif script_.status.is_not_yet_processing:
                n_wait += 1
            elif script_.status.is_now_processing_script:
                n_running += 1
            elif script_.status.is_accepted_script:
                n_accepted += 1
        return (n_tot, n_wait, n_running, n_accepted, n_failed)

    @app.template_global("count_children")
    def count_children(element):
        return len(list(element.children()))

    @app.template_global("get_attribute")
    def get_attribute(element, attr):
        return getattr(element, attr)

    @app.template_global("count_errors")
    def count_errors(element):
        n = 0
        for job_ in element.jobs_:
            n += len(job_.errors_)
        return n

    @app.template_global("count_jobs")
    def count_jobs(jobs):
        n_tot = 0
        n_accepted = 0
        n_rescuable = 0
        n_failed = 0
        n_running = 0
        n_review = 0
        n_wait = 0
        for job_ in jobs:
            n_tot += 1
            if job_.status.is_bad:
                n_failed += 1
            elif job_.status.is_not_yet_processing:
                n_wait += 1
            elif job_.status.is_now_processing:
                n_running += 1
            elif job_.status.is_reviewable:
                n_review += 1
            elif job_.status.is_accepted:
                n_accepted += 1
            elif job_.status.is_rescuable:
                n_rescuable += 1
        return (n_tot, n_wait, n_running, n_review, n_accepted, n_rescuable, n_failed)

    @app.template_global("child_status_tuple")
    def child_status_tuple(element):
        n_tot = 0
        n_accepted = 0
        n_rescuable = 0
        n_failed = 0
        n_running = 0
        n_review = 0
        n_wait = 0
        for child_ in element.children():
            n_tot += 1
            if child_.status.is_bad:
                n_failed += 1
            elif child_.status.is_not_yet_processing:
                n_wait += 1
            elif child_.status.is_now_processing:
                n_running += 1
            elif child_.status.is_reviewable:
                n_review += 1
            elif child_.status.is_accepted:
                n_accepted += 1
            elif child_.status.is_rescuable:
                n_rescuable += 1
        return (n_tot, n_wait, n_running, n_review, n_accepted, n_rescuable, n_failed)

    @app.template_global("count_scripts")
    def count_scripts(scripts):
        n_tot = 0
        n_accepted = 0
        n_failed = 0
        n_running = 0
        n_other = 0
        for script_ in scripts:
            n_tot += 1
            if script_.status in [
                StatusEnum.failed,
                StatusEnum.rejected,
                StatusEnum.rescuable,
                StatusEnum.reviewable,
            ]:
                n_failed += 1
            elif script_.status in [
                StatusEnum.waiting,
                StatusEnum.ready,
                StatusEnum.preparing,
                StatusEnum.prepared,
                StatusEnum.collectable,
                StatusEnum.collecting,
                StatusEnum.validating,
            ]:
                n_other += 1
            elif script_.status in [StatusEnum.running]:
                n_running += 1
            elif script_.status in [StatusEnum.accepted, StatusEnum.completed]:
                n_accepted += 1
        return (n_tot, n_accepted, n_failed, n_running, n_other)

    @app.template_global("count_children")
    def count_children(element):
        return len(list(element.children()))

    @app.route("/")
    def index() -> str:
        env = os.environ
        return render_template("index.html", db_url=dbi.db_url, env=env)

    @app.route("/all_confifs", methods=["GET", "POST"])
    def all_configs() -> str:
        configs = list(dbi.get_table(TableEnum.config))
        if request.method == "POST":
            action = request.form.get("action")
            if action == "load":
                return redirect(url_for("load_config"))
        return render_template("all_configs.html", db_url=dbi.db_url, configs=configs)

    @app.route("/load_config", methods=["GET", "POST"])
    def load_config() -> str:
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                config_name = request.form.get("config_name")
                config_yaml = request.form.get("config_yaml")
                dbi.parse_config(config_name, config_yaml)
            return redirect(url_for("all_configs"))
        field_dict = dict(
            config_name=dict(label="Config Name", default=""),
            config_yaml=dict(label="Config Yaml", default=""),
        )
        return render_template("load_config.html", db_url=dbi.db_url, field_dict=field_dict)

    @app.route("/config_table/<int:element_id>")
    def config_table(element_id: int) -> str:
        config = dbi.get_config_by_id(element_id)
        fragments = [assoc_.frag_ for assoc_ in config.assocs_]
        return render_template("config_tableview.html", config=config, fragments=fragments)

    @app.route("/all_productions", methods=["GET", "POST"])
    def all_productions() -> str:
        productions = list(dbi.get_table(TableEnum.production))
        if request.method == "POST":
            action = request.form.get("action")
            if action == "insert_production":
                return redirect(url_for("insert_production"))
        actions = ["insert_production"]
        return render_template(
            "all_productions.html", db_url=dbi.db_url, productions=productions, actions=actions
        )

    @app.route("/insert_production", methods=["GET", "POST"])
    def insert_production() -> str:
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                p_name = request.form.get("p_name")
                dbi.insert(None, None, None, production_name=p_name)
            return redirect(url_for("all_productions"))
        field_dict = dict(
            p_name=dict(
                label="Production Name",
                default="",
            )
        )
        return render_template("insert_element.html", db_url=dbi.db_url, field_dict=field_dict)

    @app.route("/error_trend/<level>/<int:error_type_id>")
    def error_trend(level: str, error_type_id: int) -> str:
        levelEnum = LevelEnum[level]
        trend_dict = dbi.get_error_trend_dict(error_type_id, levelEnum)
        return render_template(
            "error_trend.html", error_type_id=error_type_id, level=level, trend_dict=trend_dict
        )

    @app.route("/all_error_types", methods=["GET", "POST"])
    def all_error_types() -> str:
        if request.method == "POST":
            action = request.form.get("action")
            if action == "load":
                return redirect(url_for("load_error_types"))
            if action == "insert":
                return redirect(url_for("insert_error_type"))
            elif action == "rematch":
                dbi.rematch_errors()

        actions = ["load", "insert", "rematch"]
        error_types = list(dbi.get_all_error_types())
        return render_template(
            "all_error_types.html", db_url=dbi.db_url, error_types=error_types, actions=actions
        )

    @app.route("/load_error_types", methods=["GET", "POST"])
    def load_error_types() -> str:
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                filepath = request.form.get("filepath")
                dbi.load_error_types(filepath)
            return redirect(url_for("all_error_types"))
        field_dict = dict(
            filepath=dict(
                label="Error Yaml File",
                default="",
            )
        )
        return render_template("load_error_types.html", db_url=dbi.db_url, field_dict=field_dict)

    @app.route("/insert_error_type", methods=["GET", "POST"])
    def insert_error_type() -> str:
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                error_name = request.form.get("error_name")
                kwargs = dict(
                    panda_err_code=request.form.get("panda_err_code"),
                    diagnostic_message=request.form.get("diagnostic_message"),
                    jira_ticket=request.form.get("jira_ticket"),
                    pipetask=request.form.get("pipetask"),
                    is_resolved=request.form.get("is_resolved"),
                    is_rescueable=request.form.get("is_rescueable"),
                    error_flavor=ErrorFlavor[request.form.get("error_flavor")],
                    action=ErrorAction[request.form.get("action")],
                    max_intensity=request.form.get("max_intensity"),
                )
                dbi.insert_error_type(error_name, **kwargs)
            return redirect(url_for("all_error_types"))
        field_dict = dict(
            error_name=dict(
                label="Name",
                default="",
            ),
            panda_err_code=dict(
                label="Panda Error Code",
                default="",
            ),
            diagnostic_message=dict(
                label="Diag. Message",
                default="",
            ),
            jira_ticket=dict(
                label="Jira Ticket",
                default="",
            ),
            pipetask=dict(
                label="Pipetask",
                default="",
            ),
            is_resolved=dict(
                label="Is Resolved",
                default=True,
            ),
            is_rescueable=dict(
                label="Is Rescueable",
                default=True,
            ),
            error_flavor=dict(
                label="Error Flavor",
                default="pipelines",
            ),
            action=dict(
                label="Action",
                default="failed_review",
            ),
            max_intensity=dict(
                label="Max. Intensity",
                default=0.001,
            ),
        )
        return render_template("insert_error_type.html", db_url=dbi.db_url, field_dict=field_dict)

    @app.route("/error_type/<int:element_id>", methods=["GET", "POST"])
    def error_type(element_id: int) -> str:
        err_type = dbi.get_error_type(element_id).scalar()
        if request.method == "POST":
            action = request.form.get("action")
            if action == "trend_campaign":
                levelEnum = LevelEnum.campaign
            elif action == "trend_step":
                levelEnum = LevelEnum.step
            elif action == "trend_group":
                levelEnum = LevelEnum.group
            elif action == "trend_workflow":
                levelEnum = LevelEnum.workflow
            return redirect(url_for("error_trend", level=levelEnum.name, error_type_id=element_id))
        attrs = [
            "id",
            "error_name",
            "panda_err_code",
            "diagnostic_message",
            "jira_ticket",
            "pipetask",
            "is_resolved",
            "is_rescueable",
            "error_flavor",
            "action",
            "max_intensity",
        ]
        actions = ["trend_campaign", "trend_step", "trend_group", "trend_workflow"]
        return render_template("error_type.html", error_type=err_type, actions=actions, attrs=attrs)

    @app.route("/table/<level>/<int:element_id>", methods=["GET", "POST"])
    def table(level: str, element_id: int) -> str:
        levelEnum = LevelEnum[level]
        dbid = dbi.dbi_id_from_level_and_element(levelEnum, element_id)
        element = dbi.get_entry(levelEnum, dbid)
        actions = TABLE_ACTIONS[levelEnum]
        if request.method == "POST":
            action = request.form.get("action")
            if action == "insert_campaign":
                assert levelEnum == LevelEnum.production
                return redirect(url_for("insert_campaign", parent_id=element_id))
            elif action == "insert_step":
                assert levelEnum == LevelEnum.campaign
                return redirect(url_for("insert_step", parent_id=element_id))
            elif action == "insert_group":
                assert levelEnum == LevelEnum.step
                return redirect(url_for("insert_group", parent_id=element_id))
            elif action == "insert_rescue":
                if levelEnum == LevelEnum.group:
                    return redirect(url_for("insert_rescue", parent_id=element_id))
                elif levelEnum == LevelEnum.workflow:
                    return redirect(url_for("insert_rescue", parent_id=element.g_.id))
                raise ValueError(f"Wrong level {levelEnum} for insert_rescue")
            elif action == "check":
                dbi.check(levelEnum, dbid)
                return redirect(url_for("table", level=level, element_id=element_id))
            elif action == "accept":
                dbi.accept(levelEnum, dbid)
                return redirect(url_for("table", level=level, element_id=element_id))
            elif action == "queue":
                dbi.queue_jobs(levelEnum, dbid)
                return redirect(url_for("table", level=level, element_id=element_id))
            elif action == "launch":
                dbi.launch_jobs(levelEnum, dbid, MAX_RUNNING)
                return redirect(url_for("table", level=level, element_id=element_id))
            elif action == "fake_run":
                dbi.fake_run(levelEnum, dbid, StatusEnum.accepted)
                return redirect(url_for("table", level=level, element_id=element_id))
            elif action == "reject":
                dbi.reject(levelEnum, dbid)
                return redirect(url_for("table", level=level, element_id=element_id))
            elif action == "supersede":
                dbi.reject(levelEnum, dbid)
                return redirect(url_for("table", level=level, element_id=element_id))
            elif action == "add_script":
                return redirect(url_for("add_script", level=level, parent_id=element_id))
        if levelEnum == LevelEnum.workflow:
            jobs = [job_ for job_ in element.jobs_ if not job_.superseded]
            return render_template(
                "workflow_tableview.html",
                workflow=element,
                jobs=jobs,
                scripts=element.scripts_,
                actions=actions,
            )
        children = [child_ for child_ in element.children() if not child_.superseded]
        return render_template(
            "element_tableview.html",
            element=element,
            children=children,
            scripts=element.scripts_,
            actions=actions,
        )

    @app.route("/insert_campaign/<int:parent_id>", methods=["GET", "POST"])
    def insert_campaign(parent_id: int) -> str:
        parent_dbid = dbi.dbi_id_from_level_and_element(LevelEnum.production, parent_id)
        parent = dbi.get_entry(LevelEnum.production, parent_dbid)
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                config_block = request.form.get("config_block")
                config_name = request.form.get("config_name")
                config = dbi.get_config(config_name)
                kwargs = dict(
                    production_name=parent.name,
                    campaign_name=request.form.get("c_name"),
                    lsst_version=request.form.get("lsst_version"),
                    butler_repo=request.form.get("butler_repo"),
                    root_coll=request.form.get("root_coll"),
                    prod_base_url=request.form.get("prod_base_url"),
                )
                dbi.insert(parent_dbid, config_block, config, **kwargs)
            return redirect(url_for("table", level="production", element_id=parent_id))
        field_dict = dict(
            c_name=dict(
                label="Campaign Name",
                default="",
            ),
            config_name=dict(label="Config Name", default=""),
            config_block=dict(label="Config Block", default="campaign"),
            butler_repo=dict(
                label="Butler Repo",
                default=os.environ.get("CM_BUTLER", "/sdf/group/rubin/repo/main"),
            ),
            root_coll=dict(
                label="Root collection",
                default=f"u/{os.environ['USER']}/cm",
            ),
            lsst_version=dict(
                label="LSST Software stack Version",
                default="",
            ),
            prod_base_url=dict(
                label="Production Area",
                default=os.environ.get("CM_PROD_URL", "output/archive"),
            ),
        )
        return render_template("insert_element.html", parent=parent, field_dict=field_dict)

    @app.route("/insert_step/<int:parent_id>", methods=["GET", "POST"])
    def insert_step(parent_id: int) -> str:
        parent_dbid = dbi.dbi_id_from_level_and_element(LevelEnum.campaign, parent_id)
        parent = dbi.get_entry(LevelEnum.campaign, parent_dbid)
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                config_block = request.form.get("config_block")
                kwargs = dict(
                    production_name=parent.p_name,
                    campaign_name=parent.c_name,
                    step_name=request.form.get("s_name"),
                    lsst_version=request.form.get("lsst_version"),
                    pipeline_yaml="",
                )
                dbi.insert_step(parent_dbid, config_block, **kwargs)
            return redirect(url_for("table", level="campaign", element_id=parent_id))
        field_dict = dict(
            s_name=dict(
                label="Step Name",
                default="",
            ),
            config_block=dict(label="Config Block", default=""),
            lsst_version=dict(
                label="LSST Software stack Version",
                default=parent.lsst_version,
            ),
            pipeline_yaml=dict(
                label="Pipeline Yaml",
                default="",
            ),
        )
        return render_template("insert_element.html", parent=parent, field_dict=field_dict)

    @app.route("/add_script/<level>/<int:parent_id>", methods=["GET", "POST"])
    def add_script(level: str, parent_id: int) -> str:
        levelEnum = LevelEnum[level]
        parent_dbid = dbi.dbi_id_from_level_and_element(levelEnum, parent_id)
        parent = dbi.get_entry(levelEnum, parent_dbid)
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                script_name = request.form.get("script_name")
                kwargs = dict()
                dbi.add_script(parent_dbid, script_name, None, **kwargs)
            return redirect(url_for("table", level=level, element_id=parent_id))
        field_dict = dict(
            script_name=dict(
                label="Script Name",
                default="",
            ),
        )
        return render_template("add_script.html", parent=parent, field_dict=field_dict)

    @app.route("/insert_group/<int:parent_id>", methods=["GET", "POST"])
    def insert_group(parent_id: int) -> str:
        parent_dbid = dbi.dbi_id_from_level_and_element(LevelEnum.step, parent_id)
        parent = dbi.get_entry(LevelEnum.step, parent_dbid)
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                config_block = request.form.get("config_block")
                config_name = request.form.get("config_name")
                config = dbi.get_config(config_name)
                kwargs = dict(
                    production_name=parent.p_name,
                    campaign_name=parent.c_name,
                    step_name=parent.s_name,
                    group_name=request.form.get("g_name"),
                    lsst_version=request.form.get("lsst_version"),
                    data_query=request.form.get("data_query"),
                    pipeline_yaml=request.form.get("pipeline_yaml"),
                )
                dbi.insert(parent_dbid, config_block, config, **kwargs)
            return redirect(url_for("table", level="step", element_id=parent_id))
        field_dict = dict(
            g_name=dict(
                label="Group Name",
                default="",
            ),
            config_block=dict(
                label="Config Block",
                default="group",
            ),
            lsst_version=dict(
                label="LSST Software stack Version",
                default=parent.lsst_version,
            ),
            data_query=dict(
                label="Data Query",
                default="",
            ),
            pipeline_yaml=dict(
                label="Pipeline Yaml",
                default=parent.pipeline_yaml,
            ),
        )
        return render_template("insert_element.html", parent=parent, field_dict=field_dict)

    @app.route("/insert_rescue/<int:parent_id>", methods=["GET", "POST"])
    def insert_rescue(parent_id: int) -> str:
        parent_dbid = dbi.dbi_id_from_level_and_element(LevelEnum.group, parent_id)
        parent = dbi.get_entry(LevelEnum.group, parent_dbid)
        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                config_block = request.form.get("config_block")
                kwargs = dict(
                    production_name=parent.p_name,
                    campaign_name=parent.c_name,
                    step_name=parent.s_name,
                    group_name=parent.g_name,
                    lsst_version=request.form.get("lsst_version"),
                    data_query=request.form.get("data_query"),
                    pipeline_yaml=request.form.get("pipeline_yaml"),
                )
                dbi.insert_rescue(parent_dbid, config_block, **kwargs)
            return redirect(url_for("table", level="group", element_id=parent_id))
        field_dict = dict(
            config_block=dict(
                label="Config Block",
                default="rescue_workflow",
            ),
            lsst_version=dict(
                label="LSST Software stack Version",
                default=parent.lsst_version,
            ),
            data_query=dict(
                label="Data Query",
                default="",
            ),
            pipeline_yaml=dict(
                label="Pipeline Yaml",
                default=parent.pipeline_yaml,
            ),
        )
        return render_template("insert_element.html", parent=parent, field_dict=field_dict)

    @app.route("/table_filtered/<level>/<int:element_id>/<status>")
    def table_filtered(level: str, element_id: int, status: str) -> str:
        levelEnum = LevelEnum[level]
        dbid = dbi.dbi_id_from_level_and_element(levelEnum, element_id)
        element = dbi.get_entry(levelEnum, dbid)
        if levelEnum == LevelEnum.workflow:
            if status == "None":
                jobs = [job_ for job_ in element.jobs_]
            else:
                jobs = [job_ for job_ in element.jobs_ if getattr(job_.status, status)]
            return render_template("workflow_tableview.html", workflow=element, jobs=jobs)
        if status == "None":
            children = [child_ for child_ in element.children()]
        else:
            children = [child_ for child_ in element.children() if getattr(child_.status, status)]
        return render_template("element_tableview.html", element=element, children=children)

    @app.route("/details/<level>/<int:element_id>")
    def details(level: str, element_id: int) -> str:
        levelEnum = LevelEnum[level]
        dbid = dbi.dbi_id_from_level_and_element(levelEnum, element_id)
        element = dbi.get_entry(levelEnum, dbid)
        attrs = attribute_dict[levelEnum]
        return render_template("element_details.html", element=element, attrs=attrs)

    @app.route("/jobs/<level>/<int:element_id>", methods=["GET", "POST"])
    def jobs(level: str, element_id: int) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        if request.method == "POST":
            requeue = request.form.get("action")
            print(f"requeue {requeue}")
        return render_template("jobs.html", element=element, jobs=element.jobs_)

    @app.route("/jobs_filtered/<level>/<int:element_id>/<status>", methods=["GET", "POST"])
    def jobs_filtered(level: str, element_id: int, status: str) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        if request.method == "POST":
            requeue = request.form.get("action")
            print(f"requeue {requeue}")
        if status == "None":
            jobs = [job_ for job_ in element.jobs_]
        else:
            jobs = [job_ for job_ in element.jobs_ if getattr(job_.status, status) is True]
        return render_template("jobs.html", element=element, jobs=jobs, status=status)

    @app.route("/jobs_filtered/<level>/<int:element_id>/<status>")
    def jobs_filtered(level: str, element_id: int, status: str) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        if status == "None":
            jobs = [job_ for job_ in element.jobs_]
        else:
            jobs = [job_ for job_ in element.jobs_ if getattr(job_.status, status)]
        return render_template("filtered_jobs.html", element=element, jobs=jobs)

    @app.route("/job/<int:job_id>")
    def job(job_id: int) -> str:
        job = dbi.get_job(job_id).scalar()
        return render_template("job.html", job=job)

    @app.route("/scripts/<level>/<int:element_id>", methods=["GET", "POST"])
    def scripts(level: str, element_id: int) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        if request.method == "POST":
            to_rerun = request.form.get("rerun")
            print(f"retrun {to_rerun}")
        return render_template("scripts.html", element=element, scripts=element.scripts_)

    @app.route("/scripts_filtered/<level>/<int:element_id>/<status>")
    def scripts_filtered(level: str, element_id: int, status: str) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        if status == "None":
            scripts = [script_ for script_ in element.scripts_]
        else:
            scripts = [script_ for script_ in element.scripts_ if getattr(script_.status, status)]
        return render_template("scripts.html", element=element, scripts=scripts, status=status)

    @app.route("/script/<int:script_id>")
    def script(script_id: int) -> str:
        script = dbi.get_script(script_id).scalar()
        return render_template("script.html", script=script)

    def _sort_errors(jobs):
        error_count = {}
        error_dict = {}
        error_list = []
        for job_ in jobs:
            for error_ in job_.errors_:
                if error_.error_name is None:
                    error_list.append(error_)
                else:
                    if error_.error_name in error_dict:
                        error_count[error_.error_name] += 1
                    else:
                        error_dict[error_.error_name] = error_.error_type_id
                        error_count[error_.error_name] = 1
        return error_count, error_dict, error_list

    def _list_errors(jobs, error_type=None):
        error_list = []
        for job_ in jobs:
            for error_ in job_.errors_:
                if error_type is None or error_.error_type_id == error_type:
                    error_list.append(error_)
        return error_list

    @app.route("/job_errors/<int:element_id>")
    def job_errors(element_id: int):
        job = dbi.get_job(element_id).scalar()
        error_count, error_dict, error_list = _sort_errors([job])
        return render_template(
            "errors_summary.html",
            element=None,
            job=job,
            error_dict=error_dict,
            error_count=error_count,
            error_list=error_list,
        )

    @app.route("/error_summary/<level>/<int:element_id>")
    def error_summary(level: str, element_id: int) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        error_count, error_dict, error_list = _sort_errors(element.jobs_)
        return render_template(
            "errors_summary.html",
            element=element,
            error_dict=error_dict,
            error_count=error_count,
            error_list=error_list,
        )

    @app.route("/error_list/<level>/<int:element_id>")
    def error_list(level: str, element_id: int) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        the_errors = _list_errors(element.jobs_)
        return render_template(
            "error_list.html",
            element=element,
            error_type=None,
            error_list=the_errors,
        )

    @app.route("/filted_error_list/<level>/<int:element_id>/<int:error_type>")
    def filtered_error_list(level: str, element_id: int, error_type: int) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        the_errors = _list_errors(element.jobs_, error_type=error_type)
        return render_template(
            "error_list.html",
            element=element,
            error_type=error_type,
            error_list=the_errors,
        )

    @app.route("/filted_job_error_list/<int:job_id>/<int:error_type>")
    def filtered_job_error_list(job_id: int, error_type: int) -> str:
        job = dbi.get_job(job_id).scalar()
        the_errors = _list_errors([job], error_type=error_type)
        return render_template(
            "error_list.html",
            element=None,
            job=job,
            error_type=error_type,
            error_list=the_errors,
        )

    @app.route("/update_values/<level>/<int:element_id>/<field>", methods=("GET", "POST"))
    def update_values(level: str, element_id: int, field: str):
        levelEnum = LevelEnum[level]
        dbid = dbi.dbi_id_from_level_and_element(levelEnum, element_id)
        element = dbi.get_entry(levelEnum, dbid)
        current_value = getattr(element, field)
        if request.method == "POST":
            action = request.form["action"]
            if action == "submit":
                return redirect(url_for("details", level=level, element_id=element_id))
        field_dict = dict(
            value=dict(
                label="Value",
                default=current_value,
            )
        )
        return render_template(
            "update_values.html", field=field, element_fullname=element.fullname, field_dict=field_dict
        )

    @app.route("/attention")
    def attention():
        elements, jobs, scripts = dbi.requires_attention()
        return render_template("attention.html", elements=elements, jobs=jobs, scripts=scripts)

    @app.route("/launch_campaign", methods=["GET", "POST"])
    def launch_campaign():
        template_file = dbi.db_url.replace(".db", "_template.yaml").replace("sqlite:///", "")
        with open(template_file) as fin:
            c_template = yaml.safe_load(fin)

        if request.method == "POST":
            action = request.form.get("action")
            if action == "submit":
                config_block = request.form.get("config_block", c_template["config_block"])
                config_name = request.form.get("config_name", c_template["config_name"])
                config_yaml = os.path.expandvars(request.form.get("config_yaml", c_template["config_yaml"]))
                error_yaml = os.path.expandvars(request.form.get("error_yaml", c_template["error_yaml"]))
                production_name = request.form.get("p_name", c_template["production_name"])
                kwargs = dict(
                    production_name=production_name,
                    campaign_name=request.form.get("c_name", c_template["campaign_name"]),
                    lsst_version=request.form.get("lsst_version", c_template["lsst_version"]),
                    butler_repo=request.form.get("butler_repo", c_template["butler_repo"]),
                    root_coll=request.form.get("root_coll", c_template["root_coll"]),
                    prod_base_url=request.form.get("prod_base_url", c_template["prod_base_url"]),
                )
                dbi.parse_config(config_name, config_yaml)
                config = dbi.get_config(config_name)
                dbi.load_error_types(error_yaml)
                prod = dbi.insert(None, "", None, production_name=production_name)
                dbi.insert(prod.db_id, config_block, config, **kwargs)

            return redirect(url_for("table", level="production", element_id=prod.id))

        field_dict = dict(
            p_name=dict(label="Production Name", default=c_template["production_name"]),
            c_name=dict(
                label="Campaign Name",
                default=c_template["campaign_name"],
            ),
            config_yaml=dict(
                label="Config Yaml",
                default=c_template["config_yaml"],
            ),
            error_yaml=dict(
                label="Error Yaml",
                default=c_template["error_yaml"],
            ),
            config_name=dict(
                label="Config Name",
                default=c_template["config_name"],
            ),
            config_block=dict(
                label="Config Block",
                default=c_template["config_block"],
            ),
            butler_repo=dict(
                label="Butler Repo",
                default=c_template["butler_repo"],
            ),
            root_coll=dict(
                label="Root collection",
                default=c_template["root_coll"],
            ),
            lsst_version=dict(
                label="LSST hsc_weeklySoftware stack Version",
                default=c_template["lsst_version"],
            ),
            prod_base_url=dict(
                label="Production Area",
                default=c_template["prod_base_url"],
            ),
        )
        elements, jobs, scripts = dbi.requires_attention()
        return render_template("launch_campaign.html", db_url=dbi.db_url, field_dict=field_dict)

    @app.route("/production_file")
    def production_file():
        path = request.args.get("path")
        abspath = os.path.abspath(os.path.expandvars(path))
        with open(abspath) as fin:
            data = fin.read()
        return render_template("text.html", content=data)

    @app.route("/update_campaign/<int:element_id>/<field>", methods=("GET", "POST"))
    def update_campaign(element_id: int, field: str):
        campaign = dbi.get_entry(LevelEnum.campaign, DbId(-1, element_id))
        current_value = getattr(campaign, field)
        if request.method == "POST":
            value = request.form["value"]
            print(value)
            return redirect(url_for("index"))

        return render_template(
            "update_values.html", field=field, element_fullname=campaign.fullname, current_value=current_value
        )

    return app
