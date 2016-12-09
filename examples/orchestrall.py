#!/usr/bin/env python3
"""
"""
import multiprocessing
from time import sleep

from services.directory.client import Directory, ServiceUnavailable
from services.directory.server import DirectoryService
from services.echo import EchoService


def spawn(fn, *args):
    print('Spawn', fn, args)
    p = multiprocessing.Process(target=fn, args=args)
    p.daemon = True
    print('Spawned:', repr(p))
    p.start()
    print('Started:', repr(p), p.pid, p.is_alive())
    return p


def main():
    directory_address = 'ipc:///tmp/directory'
    d = Directory(directory_address)
    p_directory = spawn(DirectoryService(directory_address).run)

    p_echo = spawn(EchoService('ipc:///tmp/echo').run, directory_address)
    sleep(1)

    try:
        answer = d.query_service(EchoService, {'message': 'practice'})
    except ServiceUnavailable as e:
        print(repr(e))
    else:
        print(answer)

    p_echo.terminate()
    sleep(1)

    try:
        d.query_service(EchoService, {'message': 'now should by unavailable'})
    except ServiceUnavailable:
        print('Ok, "echo" is unavailable')

    p_directory.terminate()


if __name__ == '__main__':
    main()
