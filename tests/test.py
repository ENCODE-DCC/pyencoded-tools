import pytest
import encodedcc
import os.path
# so I don't forget
# py.test test.py
# py.test with -v for verbose, or -m key/connection/etc to use the marked tests


keypairs = {
            "default":
                {"server":"https://test.encodedcc.org", "key":"keystring", "secret":"secretstring"}
            }


def test_nothing():
    assert(1)


@pytest.mark.key
def test_key():
    key = encodedcc.ENC_Key(keypairs, "default")
    assert(key)
    assert(type(key.server) is str)
    assert(type(key.authpw) is str)
    assert(type(key.authid) is str)


@pytest.mark.connection
def test_connection():
    key = encodedcc.ENC_Key(keypairs, "default")
    connection = encodedcc.ENC_Connection(key)
    assert(connection)
    assert(connection.auth)
    assert(connection.server)


@pytest.mark.get
def test_get():
    key = encodedcc.ENC_Key(keypairs, "default")
    connection = encodedcc.ENC_Connection(key)
    result = encodedcc.get_ENCODE("/profiles/", connection)
    assert(type(result) is dict)
