"""
"""
import itertools
import logging
import random
from collections import namedtuple

import transport

from z_arch import RoutedService

from . import client


logger = logging.getLogger(__name__)


REGISTER = b'register'
HEARTBEAT = b'heartbeat'
SERVICE_INFO = b'service_info'
GET_SUBSCRIPTION_ADDRESS = b'get_subscription_address'


DIRECTORY = {}
NODES_SECRETS = {}


Node = namedtuple('Node', 'id secret')


nodes_counter = itertools.count()
DIRECTORY_NODE = next(nodes_counter)


class Directory:
    pass


class DirectoryService(RoutedService):
    code = b'directory'
    client = client.DirectoryClient

    def __init__(self, address):
        self._address = address

    # def run(self, directory_address):
    def run(self):
        # Unlike other services must not resister in itself.
        # Otherwise hangup occurs

        # super().run(directory_address=self._address)

        # from services.directory.client import DirectoryClient
        # self._dir = DirectoryClient(directory_address)
        # self._node = self._dir.register(self)

        self._loop()

        # TODO override loop
        # TODO PUBlicate changes in services
        # look at expired nodes and discard them
        # alive method should update service expired param


@DirectoryService.route(REGISTER)
def register(frames):
    frames = list(frames)
    logger.debug('Register frames: %r', frames)
    about = frames[0]
    code = about[b'code']
    node_id = next(nodes_counter)
    DIRECTORY.setdefault(code, {})[node_id] = about
    NODES_SECRETS[node_id] = random.randint(10**6, 10**7)
    return [Node(node_id, NODES_SECRETS[node_id])]


@DirectoryService.route(HEARTBEAT)
def heartbeat(frames):
    return ['TODO']


@DirectoryService.route(GET_SUBSCRIPTION_ADDRESS)
def get_subscription_address(frames):
    return ['TODO']


@DirectoryService.route(SERVICE_INFO)
def service_info(frames):
    try:
        service_code = next(frames)
    except StopIteration:
        raise ReqTransportError('Service code not provided') from None

    if service_code not in DIRECTORY:
        raise transport.ZRoutedNotFound

    info = [DIRECTORY[service_code]]
    logger.debug('Service info answer: %r', info)
    return info
