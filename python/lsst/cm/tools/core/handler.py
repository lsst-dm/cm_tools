"""Base class define call-back handlers"""

import yaml

from lsst.utils import doImport

from lsst.cm.tools.core.utils import LevelEnum


class Handler:

    name = None

    def __init__(self):
        self._config_url = None
        self._config = None

    def update_config(self, **kwargs):
        config_url = kwargs.get('config_yaml', None)
        if config_url == self._config_url:
            return
        self._read_config(config_url)

    def _read_config(self, config_url):
        self._config_url = config_url
        if config_url is None:
            self._config = None
            return
        self._config = yaml.safe_load(config_url)

    @property
    def config(self):
        return self._config

    @staticmethod
    def get_handler_class(class_name):
        return doImport(class_name)

    @staticmethod
    def create(class_name):
        the_class = Handler.get_handler_class(class_name)
        return the_class()

    def insert(self, level: LevelEnum, db, **kwargs):
        self.update_config(**kwargs)
        return self._insert(level, db, **kwargs)

    def _insert(self, level: LevelEnum, db, **kwargs):
        raise NotImplementedError()

    def update(self, level: LevelEnum, db, data, itr, **kwargs):
        self.update_config(**kwargs)
        return self._update(level, db, data, itr, **kwargs)

    def _update(self, level, db, data, itr, **kwargs):
        raise NotImplementedError()
