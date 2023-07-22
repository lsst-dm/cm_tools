from flask import Flask, render_template

from ..core.db_interface import DbInterface
from ..core.dbid import DbId
from ..core.utils import LevelEnum, StatusEnum, TableEnum


def create(dbi: DbInterface) -> Flask:
    app = Flask("lsst.cm.tools.app")

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
        n_other = 0
        for job_ in jobs:
            n_tot += 1
            if job_.status in [StatusEnum.failed, StatusEnum.rejected]:
                n_failed += 1
            elif job_.status in [
                StatusEnum.waiting,
                StatusEnum.ready,
                StatusEnum.preparing,
                StatusEnum.prepared,
            ]:
                n_other += 1
            elif job_.status in [
                StatusEnum.running,
                StatusEnum.collectable,
                StatusEnum.collecting,
                StatusEnum.completed,
                StatusEnum.validating,
            ]:
                n_running += 1
            elif job_.status in [StatusEnum.reviewable]:
                n_review += 1
            elif job_.status in [StatusEnum.accepted]:
                n_accepted += 1
            elif job_.status in [StatusEnum.rescuable]:
                n_rescuable += 1
        return (n_tot, n_accepted, n_rescuable, n_failed, n_running, n_review, n_other)

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
        productions = list(dbi.get_table(TableEnum.production))
        return render_template("index.html", productions=productions)

    @app.route("/production_table/<int:element_id>")
    def production_table(element_id: int) -> str:
        production = dbi.get_entry(LevelEnum.production, DbId(element_id))
        return render_template("element_tableview.html", element=production, children=production.c_)

    @app.route("/campaign_table/<int:element_id>")
    def campaign_table(element_id: int) -> str:
        campaign = dbi.get_entry(LevelEnum.campaign, DbId(-1, element_id))
        return render_template("element_tableview.html", element=campaign, children=campaign.s_)

    @app.route("/step_table/<int:element_id>")
    def step_table(element_id: int) -> str:
        step = dbi.get_entry(LevelEnum.step, DbId(-1, -1, element_id))
        return render_template("element_tableview.html", element=step, children=step.g_)

    @app.route("/group_table/<int:element_id>")
    def group_table(element_id: int) -> str:
        group = dbi.get_entry(LevelEnum.group, DbId(-1, -1, -1, element_id))
        return render_template("element_tableview.html", element=group, children=group.w_)

    @app.route("/workflow_table/<int:element_id>")
    def workflow_table(element_id: int) -> str:
        workflow = dbi.get_entry(LevelEnum.workflow, DbId(-1, -1, -1, -1, element_id))
        return render_template("workflow_tableview.html", workflow=workflow, jobs=workflow.jobs_)

    @app.route("/campaign_details/<int:element_id>")
    def campaign_details(element_id: int) -> str:
        campaign = dbi.get_entry(LevelEnum.campaign, DbId(-1, element_id))
        attrs = [
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
        ]
        return render_template("element_details.html", element=campaign, attrs=attrs)

    @app.route("/step_details/<int:element_id>")
    def step_details(element_id: int) -> str:
        step = dbi.get_entry(LevelEnum.step, DbId(-1, -1, element_id))
        attrs = [
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
        ]
        return render_template("element_details.html", element=step, attrs=attrs)

    @app.route("/group_details/<int:element_id>")
    def group_details(element_id: int) -> str:
        group = dbi.get_entry(LevelEnum.group, DbId(-1, -1, -1, element_id))
        attrs = [
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
        ]
        return render_template("element_details.html", element=group, attrs=attrs)

    @app.route("/workflow_details/<int:element_id>")
    def workflow_details(element_id: int) -> str:
        workflow = dbi.get_entry(LevelEnum.workflow, DbId(-1, -1, -1, -1, element_id))
        attrs = [
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
        ]
        return render_template("element_details.html", element=workflow, attrs=attrs)

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
            "action",
            "max_intensity",
        ]
        return render_template("error_details.html", element=error_type, attrs=attrs)

    @app.route("/job/<int:job_id>")
    def job(job_id: int) -> str:
        job = dbi.get_job(job_id).scalar()
        return render_template("job.html", job=job)

    def _element_errors(element):
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

    @app.route("/campaign_errors/<int:element_id>")
    def campaign_errors(element_id: int) -> str:
        campaign = dbi.get_entry(LevelEnum.campaign, DbId(-1, element_id))
        return _element_errors(campaign)

    @app.route("/step_errors/<int:element_id>")
    def step_errors(element_id: int) -> str:
        step = dbi.get_entry(LevelEnum.step, DbId(-1, -1, element_id))
        return _element_errors(step)

    @app.route("/group_errors/<int:element_id>")
    def group_errors(element_id: int) -> str:
        group = dbi.get_entry(LevelEnum.group, DbId(-1, -1, -1, element_id))
        return _element_errors(group)

    @app.route("/workflow_errors/<int:element_id>")
    def workflow_errors(element_id: int) -> str:
        workflow = dbi.get_entry(LevelEnum.workflow, DbId(-1, -1, -1, -1, element_id))
        return _element_errors(workflow)

    return app
