#!/usr/bin/env python3
"""
"""
import logging
import multiprocessing
import sys
from time import sleep

from services.directory.client import DirectoryClient, ServiceUnavailable
from services.directory.server import DirectoryService
from services.echo import EchoService

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


def spawn(fn, *args):
    logger.info('Spawn %r %r', fn, args)
    p = multiprocessing.Process(target=fn, args=args)
    p.daemon = True
    logger.info('Spawned: %r', p)
    p.start()
    logger.info('Started: %r %s %r', p, p.pid, p.is_alive())
    return p


def main():
    directory_address = 'ipc:///tmp/directory'
    d = DirectoryClient(directory_address)
    p_directory = spawn(DirectoryService(directory_address).run)

    p_echo = spawn(EchoService('ipc:///tmp/echo').run, directory_address)
    sleep(1)

    try:
        answer = d.query_service(EchoService, {'message': 'practice'})
    except ServiceUnavailable as e:
        logger.info(repr(e))
    else:
        logger.info(answer)

    p_echo.terminate()
    sleep(1)

    try:
        d.query_service(EchoService, {'message': 'now should by unavailable'})
    except ServiceUnavailable:
        logger.info('Ok, "echo" is unavailable')

    p_directory.terminate()


if __name__ == '__main__':
    main()