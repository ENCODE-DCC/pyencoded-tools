import pytest
import encodedcc
import os.path

def test_nothing():
    assert(1)

def test_key():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key)

def test_key_server():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key.server)

def test_key_aithid():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key.authid)

def test_key_authpw():
    key = encodedcc.ENC_Key(os.path.expanduser("~/keypairs.json"), "default")
    assert(key.authpw)
