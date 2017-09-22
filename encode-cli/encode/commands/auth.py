from . import encodedcc


class Auth:
    def __init__(self, keyfile, key):
        self.key = encodedcc.ENC_Key(keyfile, key)
        self.connection = encodedcc.ENC_Connection(self.key)
