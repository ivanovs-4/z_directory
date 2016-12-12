"""
"""
import heapq
import itertools
import logging
import random
from collections import namedtuple

import transport

from z_arch import RoutedService, _get_now_ms, RoutedInterface

from . import client

logger = logging.getLogger(__name__)


Node = namedtuple('Node', 'id secret')


class Directory:
    def __init__(self):
        self._nodes_counter = itertools.count(start=1)
        self._nodes_by_code = {}
        self._codes_by_node = {}
        self._expiration = {}
        self._expired_nodes = set()
        self._expiration_to_check = []

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
        nodes = {
            n: about for n, about in (self._nodes_by_code.get(code) or {}).items()
            if n not in self._expired_nodes
        }
        if not nodes:
            return None
        return {
            node.id: {
                **about,
                b'expired_after_ms': self._expiration[node] - _get_now_ms(),
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
        exp = self._expiration[node] = _get_now_ms() + about[b'ttl_ms']
        heapq.heappush(self._expiration_to_check, (exp, node))
        self._expired_nodes.discard(node)
        logger.debug('_update_expire_dt: %r %r', node, exp)

    def clean_expired(self):
        now = _get_now_ms()
        while self._expiration_to_check and self._expiration_to_check[0][0] <= now:
            exp, node = heapq.heappop(self._expiration_to_check)
            if self._expiration[node] <= now:
                logger.info('Service ttl expired: %r %r', node, exp)
                self._expired_nodes.add(node)

    def ms_to_next_check(self):
        if not self._expiration_to_check:
            return None
        next_exp, node = self._expiration_to_check[0]
        return next_exp - _get_now_ms()


class RegisterMethod(RoutedInterface):
    def reply(self, service, frames):
        frames = list(frames)
        logger.debug('Register frames: %r', frames)
        about = frames[0]
        node = service._directory.new_node(about[b'code'], about)
        return [node]


class HeartbeatMethod(RoutedInterface):
    def reply(self, service, frames):
        node = Node(*next(frames))
        service._directory.alive_node(node)
        return []


class ServiceInfoMethod(RoutedInterface):
    def reply(self, service, frames):
        try:
            service_code = next(frames)
        except StopIteration:
            raise ReqTransportError('Service code not provided') from None

        logger.debug('Service info code: %r', service_code)
        about = service._directory.get_service_about(service_code)
        if not about:
            raise transport.ZServiceNotFound

        info = [about]
        logger.debug('Service info answer: %r', info)
        return [about]


class GetSubscriptionAddressMethod(RoutedInterface):
    def reply(self, service, frames):
        return ['TODO']


class DirectoryService(RoutedService):
    code = b'directory'
    client = client.DirectoryClient
    interfaces = {
        'register': RegisterMethod(b'register'),
        'heartbeat': HeartbeatMethod(b'heartbeat'),
        'service_info': ServiceInfoMethod(b'service_info'),
        'get_subscription_address': GetSubscriptionAddressMethod(b'get_subscription_address'),
    }

    def __init__(self, address):
        self._directory = Directory()
        super().__init__(address=address, ttl=None)

    def _handle_alive_sending(self):
        pass

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

    def _each_loop(self):
        self._directory.clean_expired()

    def _get_loop_timeout_ms(self):
        return self._directory.ms_to_next_check()
