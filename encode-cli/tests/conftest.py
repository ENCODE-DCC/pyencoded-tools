import pytest
import os

from click.testing import CliRunner


@pytest.fixture(scope='function')
def runner(request):
    return CliRunner()


@pytest.fixture()
def keyfile():
    return os.path.expanduser('~/keypairs.json')


@pytest.fixture()
def key():
    return 'prod'
