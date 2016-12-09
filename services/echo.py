"""
"""
import logging

from services import directory
from z_arch import ZService

logger = logging.getLogger(__name__)


class EchoService(ZService):
    code = b'echo'
    # client = EchoClient

    def __init__(self, address):
        self._address = address

    def run(self, directory_address):
        # 1. connect to directory, say who we are
        _dir = directory.client.DirectoryClient(directory_address)
        node = _dir.register(self)
        logger.debug('Registered: %s', node)

        """
        2. bind aur service socket and reply to it,
        meantime send alive to directory
        """

        import zmq
        ctx = zmq.Context()
        with ctx.socket(zmq.REP) as rep:
            rep.bind(self._address)
            while True:
                handle(rep, rep.recv_multipart())

    def about(self):
        # TODO use Directory protocol, methods
        return {
            b'code': self.code,
            b'rep_address': self._address,
        }


import itertools
echo_counter = itertools.count()


def just_echo(frames):
    return [next(echo_counter)] + list(frames)


def handle(rep, received):
    # TODO move this to transport
    import msgpack
    from transport import ReqRepOk, ReqRepError, ReqRepTransportError

    request_frames = (msgpack.loads(f, encoding='utf-8', use_list=False)
                      for f in received)
    handler = just_echo
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
