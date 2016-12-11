"""
"""
import logging

import zmq

logger = logging.getLogger(__name__)


ctx = zmq.Context()


class ZReqRepOk:
    code = b'200'


class RegisteredByCode(type):
    def __init__(self, *args, **kwargs):
        if self.__name__ != 'ZReqRepError':
            if self.code in self._by_code:
                raise ValueError
            self._by_code[self.code] = self
        super().__init__(*args, **kwargs)


class ZReqRepError(Exception, metaclass=RegisteredByCode):
    _by_code = {}

    @classmethod
    def from_code(cls, code):
        return cls._by_code[code]


class ReqTransportError(ZReqRepError):
    code = b'400'


class ZRoutedMethodNotFound(ZReqRepError):
    code = b'401'


class ZServiceNotFound(ZReqRepError):
    code = b'404'


class ZRepInternalError(ZReqRepError):
    code = b'500'


class ReqTransport:
    def __init__(self, address):
        self._address = address

    def req(self, *frames):
        # TODO cache socket
        with ctx.socket(zmq.REQ) as s:
            s.connect(self._address)

            logger.debug('Send request frames: %r to address %r', frames, self._address)

            s.send_multipart(frames)
            received_frames = s.recv_multipart()

            logger.debug('Received response frames: %r from address %r', received_frames, self._address)
            return self._process_response_code(iter(received_frames))

    def _process_response_code(self, response_frames):
        try:
            response_code = next(response_frames)
        except StopIteration:
            raise ZProtocolError('No response code') from None
        if response_code != ZReqRepOk.code:
            raise ZReqRepError.from_code(response_code)
        return response_frames


    @staticmethod
    def rep(rep_socket, frames):
            rep_socket.send_multipart(frames)


class ZProtocolError(Exception):
    pass
