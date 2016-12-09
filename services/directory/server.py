"""
"""
import itertools
import logging
import random
from collections import namedtuple

import msgpack
import zmq

from transport import ReqRepNotFound


ctx = zmq.Context()

logger = logging.getLogger(__name__)


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
    logger.debug('Register frames: %r', frames)
    about = frames[0]
    code = about[b'code']
    node_id = next(nodes_counter)
    DIRECTORY.setdefault(code, {})[node_id] = about
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
    logger.debug('Service info frames: %r', frames)
    service_code = frames[0]
    if service_code not in DIRECTORY:
        raise ReqRepNotFound
    info = [DIRECTORY[service_code]]
    logger.debug('Service info answer: %r', info)
    return info


def loop(address):
    with ctx.socket(zmq.REP) as rep:
        rep.bind(address)
        while True:
            handle(rep, rep.recv_multipart())
            # TODO PUBlicate changes in services


def handle(rep, received):
    # TODO move this to transport
    from transport import ReqRepOk, ReqRepError, ReqRepTransportError

    request_frames = (msgpack.loads(f, encoding='utf-8', use_list=False)
                      for f in received)
    method_name = next(request_frames)
    handler = router.get(method_name)
    if handler:
        try:
            response_frames = handler(list(request_frames))
        except ReqRepError as e:
            response_frames = [ReqRepTransportError.code] + [
                msgpack.dumps(p, use_bin_type=True) for p in [repr(e).encode('utf-8')]]
        else:
            response_frames = [ReqRepOk.code] + [
                msgpack.dumps(p, use_bin_type=True) for p in response_frames
            ]
    else:
        response_frames = [ReqRepNotFound.code]
    rep.send_multipart(response_frames)


class DirectoryService:
    code = b'directory'

    def __init__(self, address):
        self._address = address

    def run(self, directory_address=None):
        """
        Should it make so ?

        _dir = directory.client.DirectoryClient(directory_address)
        node = _dir.register(self)

        """
        loop(self._address)
