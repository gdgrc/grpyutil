from .conn import *
import redis


class Redis(object):
    def __init__(self, address, port, db):
        self._address = address
        self._port = int(port)
        self._db = int(db)
