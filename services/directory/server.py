"""
"""
import itertools
import logging
import random
from collections import namedtuple
from datetime import datetime, timedelta

import transport

from z_arch import RoutedService

from . import client


logger = logging.getLogger(__name__)


REGISTER = b'register'
HEARTBEAT = b'heartbeat'
SERVICE_INFO = b'service_info'
GET_SUBSCRIPTION_ADDRESS = b'get_subscription_address'


class DirectoryService(RoutedService):
    code = b'directory'
    client = client.DirectoryClient

    def __init__(self, address):
        super().__init__(address=address, ttl=None)

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


Node = namedtuple('Node', 'id secret')


class Directory:
    def __init__(self):
        self._nodes_counter = itertools.count(start=1)
        self._nodes_by_code = {}
        self._codes_by_node = {}
        self._expiration = {}

    def new_node(self, code, about):
        node = Node(
            next(self._nodes_counter),
            random.randint(10**6, 10**7)
        )
        self._nodes_by_code.setdefault(code, {})[node] = about
        self._codes_by_node[node] = code
        self._update_expire_dt(node)
        return node

    def get_service_about(self, code):
        nodes = self._nodes_by_code.get(code)
        if not nodes:
            return None
        return {
            node.id: {
                **about,
                b'expired_after_ms': int((self._expiration[node] - datetime.now()).total_seconds() * 1000),
            }
            for node, about in nodes.items()
        }

    def get_about_by_node(self, node):
        code = self._codes_by_node.get(node)
        if not code:
            return None  # Skip unknown node. Or say that it is not registered?
        return self._nodes_by_code[code].get(node)

    def alive_node(self, node):
        self._update_expire_dt(node)

    def _update_expire_dt(self, node):
        about = self.get_about_by_node(node)
        if not about:
            return  # Skip unknown node. Or say that it is not registered?
        self._expiration[node] = datetime.now() + timedelta(milliseconds=about[b'ttl_ms'])
        logger.debug('_update_expire_dt: %r %r', node, self._expiration[node])

    def clean_expired(self):
        pass


directory = Directory()


@DirectoryService.route(REGISTER)
def register(frames):
    frames = list(frames)
    logger.debug('Register frames: %r', frames)
    about = frames[0]
    node = directory.new_node(about[b'code'], about)
    return [node]


@DirectoryService.route(HEARTBEAT)
def heartbeat(frames):
    node = Node(*next(frames))
    directory.alive_node(node)
    return []


@DirectoryService.route(GET_SUBSCRIPTION_ADDRESS)
def get_subscription_address(frames):
    return ['TODO']


@DirectoryService.route(SERVICE_INFO)
def service_info(frames):
    try:
        service_code = next(frames)
    except StopIteration:
        raise ReqTransportError('Service code not provided') from None

    logger.debug('Service info code: %r', service_code)
    about = directory.get_service_about(service_code)
    if not about:
        raise transport.ZRoutedNotFound

    info = [about]
    logger.debug('Service info answer: %r', info)
    return [about]
