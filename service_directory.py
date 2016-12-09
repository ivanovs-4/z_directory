#!/usr/bin/env python3
"""
"""

import itertools
import random
from collections import namedtuple

import msgpack
import zmq

from transport import ReqRepNotFound


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


DIRECTORY = {}
NODES_SECRETS = {}


Node = namedtuple('Node', 'id secret')


nodes_counter = itertools.count()
DIRECTORY_NODE = next(nodes_counter)


@router.add(REGISTER)
def register(frames):
    about = frames[0]
    code = about[b'code']
    node_id = next(nodes_counter)
    DIRECTORY[node_id] = about
    NODES_SECRETS[node_id] = random.randint(10**6, 10**7)
    return [Node(node_id, NODES_SECRETS[node_id])]


@router.add(HEARTBEAT)
def heartbeat(frames):
    return ['TODO']


@router.add(GET_SUBSCRIPTION_ADDRESS)
def get_subscription_address(frames):
    return ['TODO']


@router.add(SERVICE_INFO)
def service_info(frames):
    service_code = frames[0]
    if service_code not in DIRECTORY:
        raise ReqRepNotFound
    return [DIRECTORY[service_code]]


def loop(address):
    with ctx.socket(zmq.REP) as rep:
        rep.bind(address)
        while True:
            handle(rep, rep.recv_multipart())
            # TODO PUBlicate changes in services


def handle(rep, received):
    # TODO move this to transport
    from transport import ReqRepOk, ReqRepError, ReqRepTransportError

    request_frames = iter(received)
    method_name = next(request_frames)
    handler = router.get(method_name)
    if handler:
        try:
            response_frames = handler([msgpack.loads(f) for f in request_frames])
        except ReqRepError as e:
            response_frames = [ReqRepTransportError.code] + [
                msgpack.dumps(p) for p in [repr(e).encode('utf-8')]]
        else:
            response_frames = [ReqRepOk.code] + [msgpack.dumps(p) for p in response_frames]
    else:
        response_frames = [ReqRepNotFound.code]
    rep.send_multipart(response_frames)


class DirectoryService:
    def __init__(self, address):
        self._address = address

    def run(self):
        loop(self._address)


if __name__ == '__main__':
    import multiprocessing
    from time import sleep

    from service_echo import EchoService
    from directory_client import Directory, ServiceUnavailable

    def spawn(fn, *args):
        print('Spawn', fn, args)
        p = multiprocessing.Process(target=fn, args=args)
        p.daemon = True
        print('Spawned:', repr(p))
        p.start()
        print('Started:', repr(p), p.pid, p.is_alive())
        return p

    directory_address = 'ipc:///tmp/directory'
    directory = Directory(directory_address)
    p_directory = spawn(DirectoryService(directory_address).run)

    p_echo = spawn(EchoService('ipc:///tmp/echo').run, directory_address)
    sleep(1)

    try:
        answer = directory.query_service(EchoService, {'message': 'practice'})
    except ServiceUnavailable as e:
        print(repr(e))
    else:
        print(answer)

    p_echo.terminate()
    sleep(1)

    try:
        directory.query_service(EchoService, {'message': 'now should by unavailable'})
    except ServiceUnavailable:
        print('Ok, "echo" is unavailable')

    p_directory.terminate()
