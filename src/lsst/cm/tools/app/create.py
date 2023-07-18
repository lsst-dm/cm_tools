from flask import Flask, render_template

from ..core.db_interface import DbInterface
from ..core.utils import TableEnum


def create(dbi: DbInterface) -> Flask:
    app = Flask("lsst.cm.tools.app")

    @app.route("/")
    def index() -> str:
        productions = list(dbi.get_table(TableEnum.production))
        return render_template("index.html", productions=productions)

    @app.route("/production/<int:production_id>")
    def production(production_id: int) -> str:
        campaigns = list(dbi.get_table(TableEnum.campaign))
        return render_template("production.html", campaigns=campaigns)

    return app
