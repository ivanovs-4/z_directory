"""
"""
import logging

import transport
from z_arch import RoutedClient

logger = logging.getLogger(__name__)


class ServiceUnavailable(Exception):
    pass


class DirectoryClient(RoutedClient):
    """
    Reflect realtime directory and services changes
    """

    def __init__(self, directory_address):
        """
        Connect to directory_address, TODO subscribe to service changes
        """
        # Or maybe directory=self ?
        super().__init__(directory=None, directory_address=directory_address)

    def register(self, zservice):
        answer = self._send('register', zservice.about())
        node = next(answer)
        logger.debug('node params: %r', node)
        return server.Node(*node)

    def send_alive(self, node):
        answer = self._send('heartbeat', node)

    def _service_info(self, service):
        try:
            return list(self._send('service_info', service.code))
        except transport.ZReqRepError as rer:
            raise ServiceUnavailable from rer

    def get_client_for(self, service):
        s_info = self._service_info(service)
        logger.debug('service %r info: %r', service, s_info)
        return service.client.construct_client_from_s_info(self, s_info)

    def dump_full_info(self):
        return list(self._send('dump_full_info'))


from services.directory import server
