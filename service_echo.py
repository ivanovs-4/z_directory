#!/usr/bin/env python3
import msgpack
import zmq

from directory_client import Directory


class Echo:
    def __init__(self, address):
        self._address = address

    def run(self, directory_address):
        """
        1. connect to directory, say who we are
        directory_address = 'ipc://directory'
        directory = Directory(directory_address)
        directory.req(b'register', [{'about': 'me'}])

        2. bind aur service socket and reply to it,
        meantime send heartbeat to directory
        """
