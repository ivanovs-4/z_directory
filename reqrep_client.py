#!/usr/bin/env python3
import msgpack
import zmq


ctx = zmq.Context()


def socket_send_seq(socket, seq, flags=0):
    for f in seq[:-1]:
        s.send(f, flags=flags|zmq.MORE)
    s.send(seq[-1], flags=flags)


def socket_consume_seq(socket, flags=0):
    answer = s.recv(flags=flags)
    while answer.MORE:
        yield answer
        answer = s.recv(flags=flags)
    yield answer


class ReqRepClient:
    _packer = msgpack

    def __init__(self, address):
        self._address = address

    def _pack_frames(self, frames):
        """ Should return nonempty iterable  or raise """
        for f in frames:
            yield self._packer.dumps(f)

    def _unpack_frames(self, frames):
        for f in frames:
            yield self._packer.loads(f)

    def req(self, question):
        frames = list(self._pack_frames(iter(question)))
        # TODO cache socket
        with ctx.socket(zmq.REQ) as s:
            s.connect(self._address)
            socket_send_seq(s, frames)
            answer = socket_consume_seq(s)
        return self._unpack_frames(answer)
