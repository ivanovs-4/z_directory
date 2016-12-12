import logging
import time

import msgpack
import zmq

import transport
from transport import ReqTransport


logger = logging.getLogger(__name__)

ctx = zmq.Context()


def dump(value):
    return msgpack.dumps(value, use_bin_type=True)


def load(value):
    # logger.debug('load: %r', value)
    return msgpack.loads(value, encoding='utf-8', use_list=False)


def _get_now_ms():
    return int(time.time() * 1000)


class Z:
    pass


class Interface(Z):
    """
    Incapsulates both client request and server response
    """

    def send(self, req_address, frames):
        raise NotImplementedError

    def reply(self, service, frames):
        raise NotImplementedError


class ReqRepNonSerializedInterface(Interface):
    """Should return iterator over frames"""
    def send(self, req_address, frames):
        t = transport.ReqTransport(req_address)
        return t.req(frames)


class ReqRepInterface(ReqRepNonSerializedInterface):
    def send(self, req_address, frames):
        response_frames = super().send(req_address, [
            dump(f) for f in frames
        ])
        return (load(f) for f in response_frames)


class RoutedInterface(ReqRepNonSerializedInterface):
    def __init__(self, method_code):
        self.method_code = method_code

    def send(self, req_address, frames):
        request_frames = [self.method_code, *[dump(f) for f in frames]]
        response_frames = super().send(req_address, request_frames)
        return (load(f) for f in response_frames)


class ZClient(Z):
    pass


class ZReqRepClient(ZClient):
    def __init__(self, directory, directory_address):
        # self._directory = directory
        self._directory_address = directory_address

    def send(self, *args):
        return self.service.interface.send(self._directory_address, args)

    @classmethod
    def construct_client_from_s_info(cls, self, s_info):
        first_frame = s_info[0]
        nodes_by_id = first_frame
        about = list(nodes_by_id.values())[0]
        return cls(self, about[b'rep_address'])


class RoutedClient(ZReqRepClient):
    def _send(self, interface, *args):
        logger.debug('------- _send: %r', args)
        return self.service.interfaces[interface].send(
            self._directory_address, args)


class ZService(Z):
    client = ZClient

    def __init__(self, ttl):
        self._ttl_ms = ttl and int(ttl * 1000)

    def run(self, directory_address):
        raise NotImplementedError

    @property
    def code(self):
        raise NotImplementedError

    def about(self):
        return {
            b'code': self.code,
            b'ttl_ms': self._ttl_ms,
        }


class ZReqRepService(ZService):
    client = ZReqRepClient
    interface = ReqRepInterface()

    def __init__(self, address, ttl):
        super().__init__(ttl)
        self._address = address

    def about(self):
        return {
            **super().about(),
            b'rep_address': self._address,
        }

    def run(self, directory_address):
        # 1. connect to directory, say who we are
        from services.directory.client import DirectoryClient
        self._dir = DirectoryClient(directory_address)
        self._node = self._dir.register(self)
        logger.debug('Registered: %s', self._node)

        self._loop()

    def _loop(self):
        with ctx.socket(zmq.REP) as rep_socket:
            rep_socket.bind(self._address)
            poller = zmq.Poller()
            poller.register(rep_socket, zmq.POLLIN|zmq.POLLOUT)

            socket_handlers = {rep_socket: self._handle_rep_socket}

            self._last_sent_alive = _get_now_ms()

            while True:
                try:
                    self._handle_alive_sending()
                    self._each_loop()

                    timeout_ms = self._get_loop_timeout_ms()
                    logger.debug('%s Poll with timeout: %r', self.__class__.__name__, timeout_ms)

                    if timeout_ms is not None:
                        socks = dict(poller.poll(timeout_ms))
                    else:
                        socks = dict(poller.poll())

                    # logger.debug('Socks: %r', socks)

                    for socket in socks.keys():
                        self._handle_alive_sending()
                        socket_handlers[socket](socket)

                except Exception:
                    logger.traceback('Loop critical failure')

            poller.unregister(rep_socket)

    def _get_loop_timeout_ms(self):
        return (self._ttl_ms - self._elapsed_ms_from_last_send_alive()) // 3

    def _each_loop(self):
        pass

    def _handle_alive_sending(self):
        now = _get_now_ms()

        # If system clock chaned to past
        if self._last_sent_alive > now:
            self._last_sent_alive = now

        if self._elapsed_ms_from_last_send_alive() > self._ttl_ms // 3:
            self._send_alive()
            self._last_sent_alive = _get_now_ms()

    def _elapsed_ms_from_last_send_alive(self):
        return _get_now_ms() - self._last_sent_alive

    def _send_alive(self):
        logger.debug('Send alive %r', self._node)
        self._dir.send_alive(self._node)

    def _handle_rep_socket(self, rep_socket):
        try:
            received_frames = rep_socket.recv_multipart()
            logger.debug('%s Incoming request frames: %r', self.__class__.__name__, received_frames)
            response_frames = self._handle(received_frames)

        except transport.ZReqRepError as e:
            ReqTransport.rep(rep_socket, [e.code])

        except Exception as e:
            logger.error('%r %r', self.__class__.__name__, e)
            ReqTransport.rep(rep_socket, [transport.ZRepInternalError.code])

        else:
            ReqTransport.rep(
                rep_socket,
                [transport.ZReqRepOk.code, *response_frames]
            )

    def _handle(self, frames):
        request_frames = (load(f) for f in frames)
        response_frames = list(self.interface.reply(self, request_frames))
        return [dump(f) for f in response_frames]

ZReqRepClient.service = ZReqRepService


class RoutedServiceMeta(type):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client.service = self


class RoutedService(ZReqRepService, metaclass=RoutedServiceMeta):
    client = RoutedClient
    interfaces = ()

    def __init__(self, address, ttl):
        super().__init__(address, ttl)
        self._routes = {j.method_code: j for j in self.interfaces.values()}

    def _handle(self, frames):
        iframes = iter(frames)
        try:
            method_code = next(iframes)
        except StopIteration:
            raise transport.ReqTransportError from None

        handler = self._routes.get(method_code)
        if not handler:
            raise transport.ZRoutedMethodNotFound

        try:
            unpacked_request_iframes = (load(f) for f in iframes)
            response_frames = list(handler.reply(
                service=self,
                frames=unpacked_request_iframes
            ))
        except transport.ZReqRepError:
            raise
        except Exception as e:
            logger.exception('Handler error: %r', e)
            raise transport.ZRepInternalError from None

        return [dump(f) for f in response_frames]
