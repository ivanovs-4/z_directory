"""
"""

from service_directory import REGISTER, SERVICE_INFO, Node
from transport import ReqRepError
from z_arch import ZClient


class ServiceUnavailable(Exception):
    pass


class Directory(ZClient):
    """
    Reflect realtime directory and services changes
    """

    def __init__(self, address):
        """
        Connect to address, subscribe to service changes
        """
        self._address = address

    def register(self, zservice):
        node = next(self.query_raw(REGISTER, zservice.about()))
        return Node(*node)

    def _service_info(self, service):
        try:
            return list(self.query_raw(SERVICE_INFO, service.code))
        except ReqRepError as rer:
            raise ServiceUnavailable from rer

    def query_service(self, service, *args):
        s_info = self._service_info(service)
        asked_client = service.client(s_info)
        return asked_client.query_raw(*args)
