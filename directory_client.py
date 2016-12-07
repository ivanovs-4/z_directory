#!/usr/bin/env python3
import msgpack
import zmq

from .reqrep_client import ReqRepClient
from .service_directory import SERVICE_INFO, RttpError


class RttpClient(ReqRepClient):
    def _pack_frames(self, frames):
        """ Do not pack first frame. It is a method name """
        yield next(frames)
        yield from super()._pack_frames(frames)

    def _unpack_frames(self, frames):
        """ Do not pack first frame. It is a response code """
        yield next(frames)
        yield from super()._unpack_frames(frames)

    def req(self, method_name, question=()):
        answer = super().req([method_name] + list(question))
        code = next(answer)
        if code != RttpOk.code:
            raise RttpError.from_code(code)(list(answer))
        return list(answer)


class DirectoryClient(RttpClient):
    pass


# class Directory:
#     """
#     d = Directory('ipc://directory')
#     print(d.service('echo').req({'message': 'practice'}))
#     """
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


def ask(directory, service_name, question):
    try:
        s_info = directory.req(SERVICE_INFO)
    except RttpError as rer:
        raise ServiceUnavailable from rer
    s = ReqRepClient(s_info[0]['rep_address'])
    return s.req(question)


if __name__ == '__main__':
    import multiprocessing
    from . import service_directory
    sd = multiprocessing.spawn(service_directory.loop)

    # es = multiprocessing.spawn(echo_service)

    directory = DirectoryClient('ipc://directory')
    print(ask)
    answer = ask(directory, 'echo', [{'message': 'practice'}])
    print(answer)

    es.kill()
    sleep(1)
    try:
        ask(directory, 'echo', [{'message': 'now should by unavailable'}])
    except ServiceUnavailable:
        print('Ok, echo unavailable')

    sd.kill()
