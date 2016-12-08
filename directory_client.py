#!/usr/bin/env python3
import msgpack
import zmq

from service_directory import SERVICE_INFO, RttpError, RttpOk


ctx = zmq.Context()


class ReqRepClient:
    def __init__(self, address):
        self._address = address

    def req(self, *frames):
        # TODO cache socket
        with ctx.socket(zmq.REQ) as s:
            s.connect(self._address)
            s.send_multipart(frames)
            yield from iter(s.recv_multipart())


class RttpClient(ReqRepClient):
    def req(self, method_name, *args):
        frames = [method_name] + [msgpack.dumps(f) for f in args]
        answer = super().req(*frames)
        code = next(answer)
        if code != RttpOk.code:
            raise RttpError.from_code(code)(list(answer))
        for f in answer:
            yield msgpack.loads(f)


class ServiceUnavailable(Exception):
    pass


class Directory:
    """
    Show realtime directory and services shanges
    """

    def __init__(self, address):
        self._client = RttpClient(address)

    def ask_service(self, service_name, *args):
        try:
            s_info = list(self._client.req(SERVICE_INFO, service_name))
        except RttpError as rer:
            raise ServiceUnavailable from rer
        asked_client = ReqRepClient(s_info[0]['rep_address'])
        return asked_client.req(*args)

#     def __init__(self, address):
#         """
#         connect to address, subscribe to service changes
#         """
#         self._address = address
#         self._directory_service = DirectoryService(self._address)
#         self._services = {}

#     def service(self, service_name):
#         s = self._services.get(service_name)
#         if s:
#             return s

#         s_info = self._directory_service.req({
#             'method': 'WHERE_IS_SERVICE',
#             'data': {'name': service_name},
#         })
#         if s_info:
#             s = self._services[service_name] = ReqRepClient(**s_info)
#             return s

#         raise ServiceUnavailable from None
