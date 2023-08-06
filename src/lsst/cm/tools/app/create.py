import os

from flask import Flask, redirect, render_template, request, url_for

from ..core.db_interface import DbInterface
from ..core.utils import LevelEnum, StatusEnum, TableEnum

SECRET_KEY = """
I was the shadow of the waxwing slain
By the false azure in the windowpane;
I was that smudge of ashen fluff–and I
Lived on, flew on, in the reflected sky.
"""


attribute_dict = {
    LevelEnum.production: [],
    LevelEnum.campaign: [
        "id",
        "config_id",
        "frag_id",
        "fullname",
        "data_query",
        "bps_yaml_template",
        "bps_script_template",
        "coll_source",
        "coll_in",
        "coll_out",
        "coll_validate",
        "coll_ancil",
        "butler_repo",
        "lsst_version",
        "lsst_custom_setup",
        "root_coll",
        "prod_base_url",
    ],
    LevelEnum.step: [
        "id",
        "config_id",
        "frag_id",
        "fullname",
        "data_query",
        "bps_yaml_template",
        "bps_script_template",
        "coll_source",
        "coll_in",
        "coll_out",
        "coll_validate",
        "lsst_version",
        "lsst_custom_setup",
    ],
    LevelEnum.group: [
        "id",
        "config_id",
        "frag_id",
        "fullname",
        "data_query",
        "bps_yaml_template",
        "bps_script_template",
        "coll_source",
        "coll_in",
        "coll_out",
        "coll_validate",
        "lsst_version",
        "lsst_custom_setup",
    ],
    LevelEnum.workflow: [
        "id",
        "config_id",
        "frag_id",
        "fullname",
        "data_query",
        "bps_yaml_template",
        "bps_script_template",
        "pipeline_yaml",
        "coll_in",
        "coll_out",
        "coll_validate",
        "lsst_version",
        "lsst_custom_setup",
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
        n_wait = 0
        for script_ in scripts:
            n_tot += 1
            if script_.status in [
                StatusEnum.failed,
                StatusEnum.rejected,
                StatusEnum.populating,  # These states should not happen
                StatusEnum.rescuable,
                StatusEnum.reviewable,
                StatusEnum.collectable,
                StatusEnum.collecting,
                StatusEnum.validating,
            ]:
                n_failed += 1
            elif script_.status in [
                StatusEnum.waiting,
                StatusEnum.ready,
                StatusEnum.preparing,
                StatusEnum.prepared,
            ]:
                n_wait += 1
            elif script_.status in [StatusEnum.running]:
                n_running += 1
            elif script_.status in [StatusEnum.accepted, StatusEnum.completed]:
                n_accepted += 1
        return (n_tot, n_wait, n_running, n_accepted, n_failed)

    @app.template_global("count_children")
    def count_children(element):
        return len(list(element.children()))

    @app.route("/")
    def index() -> str:
        return render_template("index.html", db_url=dbi.db_url)

    @app.route("/all_confifs", methods=["GET", "POST"])
    def all_configs() -> str:
        configs = list(dbi.get_table(TableEnum.config))
        if request.method == "POST":
            load = request.form.get("load")
            if load == "load":
                return redirect(url_for("load_config"))
        return render_template("all_configs.html", db_url=dbi.db_url, configs=configs)

    @app.route("/load_config", methods=["GET", "POST"])
    def load_config() -> str:
        if request.method == "POST":
            config_name = request.form.get("config_name")
            config_yaml = request.form.get("config_yaml")
            dbi.parse_config(config_name, config_yaml)
            return redirect(url_for("all_configs"))
        return render_template("load_config.html", db_url=dbi.db_url)

    @app.route("/config_table/<int:element_id>")
    def config_table(element_id: int) -> str:
        config = dbi.get_config_by_id(element_id)
        fragments = [assoc_.frag_ for assoc_ in config.assocs_]
        return render_template("config_tableview.html", config=config, fragments=fragments)

    @app.route("/all_productions", methods=["GET", "POST"])
    def all_productions() -> str:
        productions = list(dbi.get_table(TableEnum.production))
        if request.method == "POST":
            insert = request.form.get("insert")
            if insert == "insert":
                return redirect(url_for("insert_production"))
        return render_template("all_productions.html", db_url=dbi.db_url, productions=productions)

    @app.route("/insert_production", methods=["GET", "POST"])
    def insert_production() -> str:
        if request.method == "POST":
            p_name = request.form.get("p_name")
            dbi.insert(None, None, None, production_name=p_name)
            return redirect(url_for("all_productions"))
        return render_template("insert_production.html", db_url=dbi.db_url)

    @app.route("/all_error_types")
    def all_error_types() -> str:
        error_types = list(dbi.get_all_error_types())
        return render_template("all_error_types.html", db_url=dbi.db_url, error_types=error_types)

    @app.route("/error_type/<int:element_id>", methods=["GET", "POST"])
    def error_type(element_id: int) -> str:
        error_type = dbi.get_error_type(element_id).scalar()
        return render_template("error_type.html", error_type=error_type)

    @app.route("/table/<level>/<int:element_id>", methods=["GET", "POST"])
    def table(level: str, element_id: int) -> str:
        levelEnum = LevelEnum[level]
        dbid = dbi.dbi_id_from_level_and_element(levelEnum, element_id)
        element = dbi.get_entry(levelEnum, dbid)
        if request.method == "POST":
            insert = request.form.get("insert")
            check = request.form.get("check")
            if insert == "insert":
                if levelEnum == LevelEnum.production:
                    return redirect(url_for("insert_campaign", parent_id=element_id))
            if check == "check":
                dbi.check(levelEnum, dbid)
                return redirect(url_for("table", level=level, element_id=element_id))

        if levelEnum == LevelEnum.workflow:
            return render_template("workflow_tableview.html", workflow=element, jobs=element.jobs_)
        return render_template("element_tableview.html", element=element, children=element.children())

    @app.route("/insert_campaign/<int:parent_id>", methods=["GET", "POST"])
    def insert_campaign(parent_id: int) -> str:
        parent_dbid = dbi.dbi_id_from_level_and_element(LevelEnum.production, parent_id)
        parent = dbi.get_entry(LevelEnum.production, parent_dbid)
        default_values = dict(
            config_block="campaign",
            butler_repo=os.environ.get("CM_BUTLER", "/sdf/group/rubin/repo/main"),
            root_coll=f"u/{os.environ['USER']}/cm",
            lsst_version="dummy",
            prod_base_url=os.environ.get("CM_PROD_URL", "output/archive"),
        )
        if request.method == "POST":
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
        return render_template("insert_campaign.html", parent=parent, def_values=default_values)

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

    @app.route("/error_details/<int:element_id>")
    def error_details(element_id: int) -> str:
        error_type = dbi.get_error_type(element_id).scalar()
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
        return render_template("error_details.html", element=error_type, attrs=attrs)

    @app.route("/jobs/<level>/<int:element_id>")
    def jobs(level: str, element_id: int) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        return render_template("jobs.html", element=element, jobs=element.jobs_)

    @app.route("/jobs_filtered/<level>/<int:element_id>/<status>")
    def jobs_filtered(level: str, element_id: int, status: str) -> str:
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        if status == "None":
            jobs = [job_ for job_ in element.jobs_]
        else:
            jobs = [job_ for job_ in element.jobs_ if getattr(job_.status, status)]
        return render_template("jobs.html", element=element, jobs=jobs)

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
        return render_template("scripts.html", element=element, scripts=scripts)

    @app.route("/script/<int:script_id>")
    def script(script_id: int) -> str:
        script = dbi.get_script(script_id).scalar()
        return render_template("script.html", script=script)

    @app.route("/errors/<level>/<int:element_id>")
    def errors(level: str, element_id: int):
        the_level = LevelEnum[level]
        db_id = dbi.dbi_id_from_level_and_element(the_level, element_id)
        element = dbi.get_entry(the_level, db_id)
        error_count = {}
        error_dict = {}
        error_list = []
        for job_ in element.jobs_:
            for error_ in job_.errors_:
                if error_.error_name is None:
                    error_list.append(error_)
                else:
                    if error_.error_name in error_dict:
                        error_count[error_.error_name] += 1
                    else:
                        error_dict[error_.error_name] = error_.error_type_id
                        error_count[error_.error_name] = 1
        return render_template(
            "errors_summary.html",
            element=element,
            error_dict=error_dict,
            error_count=error_count,
            error_list=error_list,
        )

    @app.route("/update_values/<level>/<int:element_id>/<field>", methods=("GET", "POST"))
    def update_values(level: str, element_id: int, field: str):
        levelEnum = LevelEnum[level]
        dbid = dbi.dbi_id_from_level_and_element(levelEnum, element_id)
        element = dbi.get_entry(levelEnum, dbid)
        current_value = getattr(element, field)
        if request.method == "POST":
            value = request.form["value"]
            if value == "value":
                return redirect(url_for("details", level=level, element_id=element_id))

        return render_template(
            "update_values.html", field=field, element_fullname=element.fullname, current_value=current_value
        )

    return app
