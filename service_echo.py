#!/usr/bin/env python3
import msgpack
import zmq

from directory_client import DirectoryClient


def main(directory_address):
    """
    1. connect to directory, say who we are
    directory_address = 'ipc://directory'
    directory = DirectoryClient(directory_address)
    directory.req(b'register', [{'about': 'me'}])

    2. bind aur service socket and reply to it,
    meantime send heartbeat to directory
    """


if __name__ == '__main__':
    main()
