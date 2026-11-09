import sys
from types import ModuleType
from unittest.mock import MagicMock

class MockModule(ModuleType):
    def __init__(self, name):
        super().__init__(name)
    def __getattr__(self, name):
        m = MagicMock()
        return m

for mod_name in [
    'dask', 'dask.dataframe', 'dask.bag', 'dask.distributed', 'dask.config',
    'impresso_commons', 'impresso_commons.utils', 'impresso_commons.utils.s3',
    'impresso_commons.utils.utils', 'impresso_essentials', 'impresso_essentials.io',
    'impresso_essentials.io.s3', 'impresso_essentials.utils', 'docopt'
]:
    sys.modules[mod_name] = MockModule(mod_name)
