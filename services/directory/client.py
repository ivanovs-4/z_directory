"""
"""
from transport import ReqRepError
from z_arch import ZClient

from . import server


class ServiceUnavailable(Exception):
    pass


class DirectoryClient(ZClient):
    """
    Reflect realtime directory and services changes
    """

    def __init__(self, address):
        """
        Connect to address, subscribe to service changes
        """
        # Or maybe directory=self ?
        super().__init__(directory=None, address=address)

    def register(self, zservice):
        node = next(self.query_raw(server.REGISTER, zservice.about()))
        return server.Node(*node)

    def _service_info(self, service):
        try:
            return list(self.query_raw(server.SERVICE_INFO, service.code))
        except ReqRepError as rer:
            raise ServiceUnavailable from rer

    def get_client_for(self, service):
        s_info = self._service_info(service)
        return service.client.construct_from_about(self, s_info)

    def query_service(self, service, *args):
        asked_client = self.get_client_for(service)
        return asked_client.query_raw(*args)
