#!/usr/bin/env python3
import msgpack
import zmq
from time import sleep

from service_directory import SERVICE_INFO, RttpError, RttpOk


ctx = zmq.Context()


class ReqRepClient:
    def __init__(self, address):
        self._address = address

    def req(self, frames):
        # TODO cache socket
        with ctx.socket(zmq.REQ) as s:
            s.connect(self._address)
            s.send_multipart(frames)
            yield from iter(s.recv_multipart())


class RttpClient(ReqRepClient):
    def req(self, method_name, question=()):
        frames = [method_name] + [msgpack.dumps(f) for f in question]
        answer = super().req(frames)
        code = next(answer)
        if code != RttpOk.code:
            raise RttpError.from_code(code)(list(answer))
        for f in answer:
            yield msgpack.loads(f)


class ServiceUnavailable(Exception):
    pass


class DirectoryClient(RttpClient):
    def ask_service(self, service_name, question):
        try:
            s_info = list(self.req(SERVICE_INFO, [service_name]))
        except RttpError as rer:
            raise ServiceUnavailable from rer
        s = ReqRepClient(s_info[0]['rep_address'])
        return s.req(question)


class Directory:
    """
    Show realtime directory and services shanges
    """
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


if __name__ == '__main__':
    import multiprocessing
    import service_directory
    import service_echo

    def spawn(fn, *args, daemon=True):
        p = multiprocessing.Process(target=fn, args=args)
        p.daemon = True
        p.start()
        return p

    directory_address = 'ipc://directory'
    p_directory = spawn(service_directory.loop, directory_address)

    p_echo = spawn(service_echo.main, directory_address)

    directory = DirectoryClient(directory_address)

    # answer = directory.ask_service(b'echo', [{'message': 'practice'}])
    # print(answer)

    p_echo.terminate()
    sleep(1)

    try:
        directory.ask_service('echo', [{'message': 'now should by unavailable'}])
    except ServiceUnavailable:
        print('Ok, "echo" is unavailable')

    p_directory.terminate()
