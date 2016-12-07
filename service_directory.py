#!/usr/bin/env python3
import msgpack
import zmq


ctx = zmq.Context()


class Router(dict):
    def add(self, route):
        def decorator(fn):
            self[route] = fn
        return decorator


router = Router()


REGISTER = 'REGISTER'
SERVICE_INFO = 'SERVICE_INFO'


@router.add(REGISTER)
def register(frames):
    return ['ok']


@router.add(SERVICE_INFO)
def service_info(frames):
    service_name = next(frames)
    return [{
        'name': service_name,
        'rep_address': 'to be continued',
    }]


class RttpOk:
    code = b'200'


class RttpError(Exception):
    _by_code = {}

    # def __new__(cls, *args, **kwargs):
    #     add to _by_code

    @classmethod
    def from_code(cls, code):
        return cls._by_code[code]


class RttpNotFound(RttpError):
    code = b'404'


RttpError._by_code[RttpNotFound.code] = RttpNotFound


def loop(address):
    with ctx.socket(zmq.REP) as s:
        s.connect(address)
        while True:
            poll()
            request_frames = socket_consume_req(s)
            r = next(request_frames)
            handler = router.get(r)
            if handler:
                response_frames = [RttpOk.code] + list(handler(request_frames))
            else:
                response_frames = [RttpNotFound.code]
            socket_send_rep(s, response_frames)


def main():
    loop('ipc://directory')


if __name__ == '__main__':
    main()
