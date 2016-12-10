"""
"""
import logging

from z_arch import ZReqRepClient

logger = logging.getLogger(__name__)


class ServiceUnavailable(Exception):
    pass


class DirectoryClient(ZReqRepClient):
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
        from services.directory import server
        node = next(self.query_raw(server.REGISTER, zservice.about()))
        logger.debug('node params: %r', node)
        return server.Node(*node)

    def query_service(self, service, *args):
        asked_client = self.get_client_for(service)
        return asked_client.query_raw(*args)

    def query_raw(self, method_name, *args):
        """Should return iterator over frames"""
        import transport
        # logger.info('_directory_address: %r', self._directory_address)
        t = transport.ReqTransport(self._directory_address)
        request_frames = [self._dump(f) for f in args]
        response_frames = t.req(method_name, *request_frames)
        return (self._load(f) for f in response_frames)

    def _service_info(self, service):
        import transport
        from services.directory.client import ServiceUnavailable
        from services.directory import server
        try:
            return list(self.query_raw(server.SERVICE_INFO, service.code))
        except transport.ZReqRepError as rer:
            raise ServiceUnavailable from rer

    def get_client_for(self, service):
        s_info = self._service_info(service)
        logger.debug('service %r info: %r', service, s_info)
        return service.client.construct_from_s_info(self, s_info)
