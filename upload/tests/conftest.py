import pytest
from os.path import abspath, dirname, join

from upload.settings import Default
from upload import iloop_client


@pytest.fixture(scope='session')
def examples():
    return abspath(join(dirname(abspath(__file__)), "..", "data", "examples"))


@pytest.fixture(scope='session')
def iloop():
    return iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)


@pytest.fixture(scope='session')
def project(iloop):
    return iloop.Project.first()
