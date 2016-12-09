"""
"""
import zmq
import msgpack


ctx = zmq.Context()


class ReqRepOk:
    code = b'200'


class RegisteredByCode(type):
    def __init__(self, *args, **kwargs):
        if self.__name__ != 'ReqRepError':
            if self.code in self._by_code:
                raise ValueError
            self._by_code[self.code] = self
        super().__init__(*args, **kwargs)


class ReqRepError(Exception, metaclass=RegisteredByCode):
    _by_code = {}

    @classmethod
    def from_code(cls, code):
        return cls._by_code[code]


class ReqRepTransportError(ReqRepError):
    code = b'400'


class ReqRepNotFound(ReqRepError):
    code = b'404'


class ReqRepTransport:
    def __init__(self, address):
        self._address = address

    def req(self, *frames):
        # TODO cache socket
        with ctx.socket(zmq.REQ) as s:
            s.connect(self._address)
            s.send_multipart(frames)
            return iter(s.recv_multipart())


class ReqRepMsgpackTransport(ReqRepTransport):
    def req(self, method_name, *args):
        frames = [method_name] + [msgpack.dumps(f) for f in args]
        answer = super().req(*frames)
        code = next(answer)
        if code != ReqRepOk.code:
            raise ReqRepError.from_code(code)(list(answer))
        return (msgpack.loads(f) for f in answer)
