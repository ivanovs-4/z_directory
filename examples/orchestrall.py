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

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def spawn(fn, *args, daemon=True):
    logger.info('Spawn %r %r', fn, args)
    p = multiprocessing.Process(target=fn, args=args)
    p.daemon = daemon
    logger.info('Spawned: %r', p)
    p.start()
    logger.info('Started: %r %s %r', p, p.pid, p.is_alive())
    return p


def spawner(num, directory_address):
    ps = []
    p_directory = spawn(DirectoryService(directory_address).run)

    for j in range(num):
        ps.append(spawn(EchoService('ipc:///tmp/echo_F_{}'.format(j), ttl=1).run, directory_address))
        sleep(0.3)

    sleep(10)

    for j in ps:
        j.terminate()

    sleep(30)
    p_directory.terminate()


def main():
    directory_address = 'ipc:///tmp/directory'
    d = DirectoryClient(directory_address)

    p_spawner = spawn(spawner, 12, directory_address, daemon=False)

    sleep(1)

    s_info = d._service_info(EchoService)
    logger.debug('============= service %r info: %r', EchoService, (s_info))

    answer = d.get_client_for(EchoService).send({'message': 'practice'})
    logger.info('Echo answer to main: %r', list(answer))

    answer = d.get_client_for(EchoService).send({'message': 'patience'})
    logger.info('Second answer to main: %r', list(answer))

    sleep(1)

    try:
        d.get_client_for(EchoService).send({'message': 'now will by available'})
    except ServiceUnavailable:
        logger.info('Ok, "echo" is unavailable')

    sleep(2.1)
    answer = d.get_client_for(EchoService).send({'message': 'interest'})
    logger.info('Third answer to main: %r', list(answer))
    sleep(1)

    logger.info('Full info: %r', d.dump_full_info())

    p_spawner.join()


if __name__ == '__main__':
    main()
