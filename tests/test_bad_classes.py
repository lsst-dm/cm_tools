from typing import Any

import pytest

from lsst.cm.tools.core.checker import Checker
from lsst.cm.tools.core.handler import Handler


def test_bad_checker() -> None:
    class BadChecker(Checker):
        pass

    with pytest.raises(TypeError):
        Checker.get_checker("lsst.cm.tools.core")


def test_bad_handler() -> None:
    class BadHandler(Handler):
        @classmethod
        def bad_get_kwarg(cls) -> Any:
            return cls.get_kwarg_value("bad")

        def bad_resolve_templated(self) -> None:
            self.config["bad_template"] = "{missing}"
            self.resolve_templated_string("bad_template")

    with pytest.raises(TypeError):
        Handler.get_handler("lsst.cm.tools.core.handler", "dummy.yaml")

    with pytest.raises(KeyError):
        BadHandler.bad_get_kwarg()
