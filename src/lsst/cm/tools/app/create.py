from contextlib import closing
from io import StringIO

from flask import Flask, render_template

from ..core.db_interface import DbInterface
from ..core.utils import TableEnum


def create(dbi: DbInterface) -> Flask:
    app = Flask("lsst.cm.tools.app")

    @app.route("/")
    def index() -> str:
        with closing(StringIO()) as f:
            dbi.print_table(f, TableEnum.production)
            productions = [
                f.getvalue().strip(),
                f.getvalue().strip(),
                f.getvalue().strip(),
            ]
            return render_template("index.html", productions=productions)

    return app
