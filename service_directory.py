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


REGISTER = b'register'
HEARTBEAT = b'heartbeat'
SERVICE_INFO = b'service_info'
GET_SUBSCRIPTION_ADDRESS = b'get_subscription_address'


directory = {}


@router.add(REGISTER)
def register(frames):
    return ['TODO']


@router.add(HEARTBEAT)
def heartbeat(frames):
    return ['TODO']


@router.add(GET_SUBSCRIPTION_ADDRESS)
def get_subscription_address(frames):
    return ['TODO']


@router.add(SERVICE_INFO)
def service_info(frames):
    service_name = next(frames)
    if service_name not in directory:
        raise RttpNotFound
    return [directory[service_name]]


class RttpOk:
    code = b'200'


class RegisteredByCode(type):
    def __init__(self, *args, **kwargs):
        if self.__name__ != 'RttpError':
            if self.code in self._by_code:
                raise ValueError
            self._by_code[self.code] = self
        super().__init__(*args, **kwargs)


class RttpError(Exception, metaclass=RegisteredByCode):
    _by_code = {}

    @classmethod
    def from_code(cls, code):
        return cls._by_code[code]


class RttpClientError(RttpError):
    code = b'400'


class RttpNotFound(RttpError):
    code = b'404'


def loop(address):
    with ctx.socket(zmq.REP) as rep:
        rep.bind(address)
        while True:
            received = rep.recv_multipart()
            # TODO PUBlicate changes in services
            request_frames = iter(received)
            r = next(request_frames)
            handler = router.get(r)
            if handler:
                try:
                    response_frames = handler(msgpack.loads(f) for f in request_frames)
                except RttpError as e:
                    response_frames = [RttpClientError.code] + [
                        msgpack.dumps(p) for p in [repr(e).encode('utf-8')]]
                else:
                    response_frames = [RttpOk.code] + [msgpack.dumps(p) for p in response_frames]
            else:
                response_frames = [RttpNotFound.code]
            rep.send_multipart(response_frames)


class DirectoryService:
    def __init__(self, address):
        self._address = address

    def run(self):
        loop(self._address)


if __name__ == '__main__':
    import multiprocessing
    from time import sleep

    from service_echo import Echo
    from directory_client import Directory, ServiceUnavailable

    def spawn(fn, *args, daemon=True):
        p = multiprocessing.Process(target=fn, args=args)
        p.daemon = True
        p.start()
        return p

    directory_address = 'ipc://directory'
    p_directory = spawn(DirectoryService(directory_address).run)

    p_echo = spawn(Echo('ipc://echo').run, directory_address)

    directory = Directory(directory_address)

    # answer = directory.ask_service(b'echo', {'message': 'practice'})
    # print(answer)

    p_echo.terminate()
    sleep(1)

    try:
        directory.ask_service('echo', {'message': 'now should by unavailable'})
    except ServiceUnavailable:
        print('Ok, "echo" is unavailable')

    p_directory.terminate()
