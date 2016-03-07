import pytest
import encodedcc
import os.path
# so I don't forget
# py.test test.py
# py.test with -v for verbose, or -m key/connection/etc to use the marked tests


def test_nothing():
    assert(1)


@pytest.mark.key
def test_key():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key)


@pytest.mark.key
def test_key_server():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key.server)


@pytest.mark.key
def test_key_authid():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key.authid)


@pytest.mark.key
def test_key_authpw():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key.authpw)


@pytest.mark.connection
def test_connection():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    connection = encodedcc.ENC_Connection(key)
    assert(connection)


@pytest.mark.connection
def test_connection_key():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    connection = encodedcc.ENC_Connection(key)
    assert(connection.auth)


@pytest.mark.connection
def test_connection_server():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    connection = encodedcc.ENC_Connection(key)
    assert(connection.server)


@pytest.mark.get
def test_get():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    connection = encodedcc.ENC_Connection(key)
    result = encodedcc.get_ENCODE("/profiles/", connection)
    assert(result)
