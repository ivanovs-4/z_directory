import logging
import time

import msgpack
import zmq

import transport

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
    def _dump(self, value):
        return dump(value)

    def _load(self, value):
        return load(value)


class ZClient(Z):
    pass


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


class ZReqRepClient(ZClient):
    def __init__(self, directory, directory_address):
        # self._directory = directory
        self._directory_address = directory_address

    def query_raw(self, *args):
        """Should return iterator over frames"""
        import transport
        t = transport.ReqTransport(self._directory_address)
        response_frames = t.req(*[self._dump(f) for f in args])
        return (self._load(f) for f in response_frames)

    @classmethod
    def construct_from_s_info(cls, self, s_info):
        # TODO use Directory protocol, methods
        first_frame = s_info[0]
        nodes_by_id = first_frame
        about = list(nodes_by_id.values())[0]
        return cls(self, about[b'rep_address'])


class ZReqRepService(ZService):
    client = ZReqRepClient

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

            self._last_sent_allive = _get_now_ms()

            while True:
                self._handle_alive_sending()
                self._each_loop()

                timeout_ms = self._get_loop_timeout_ms()
                logger.debug('Poll with timeout: %s', timeout_ms)
                socks = dict(poller.poll(timeout_ms))

                for socket in socks.keys():
                    self._handle_alive_sending()
                    socket_handlers[socket](socket)

            poller.unregister(rep_socket)

    def _get_loop_timeout_ms(self):
        return (self._ttl_ms - self._elapsed_ms_from_last_send_alive()) // 3

    def _each_loop(self):
        pass

    def _handle_alive_sending(self):
        now = _get_now_ms()

        # If system clock chaned to past
        if self._last_sent_allive > now:
            self._last_sent_allive = now

        if self._elapsed_ms_from_last_send_alive() > self._ttl_ms // 3:
            # send
            self._send_alive()
            self._last_sent_allive = _get_now_ms()

    def _elapsed_ms_from_last_send_alive(self):
        return _get_now_ms() - self._last_sent_allive

    def _send_alive(self):
        logger.debug('Send alive %r', self._node)
        self._dir.send_alive(self._node)

    def _handle_rep_socket(self, rep_socket):
        from transport import ReqTransport

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


class Router(dict):
    def add(self, route):
        def decorator(fn):
            self[route] = fn
            return fn
        return decorator


# TODO Need Interface abstraction that incapsulates both client request and
# server response


class AddRouterAttr(type):
    def __init__(self, *args, **kwargs):
        self._router = Router()
        super().__init__(*args, **kwargs)


class RoutedClient(ZReqRepClient):
    pass


class RoutedService(ZReqRepService, metaclass=AddRouterAttr):
    client = RoutedClient

    @classmethod
    def route(cls, method_name):
        def route_add(fn):
            return cls._router.add(method_name)(fn)
        return route_add

    def _handle(self, frames):
        iframes = iter(frames)
        try:
            method_name = next(iframes)
        except StopIteration:
            raise transport.ReqTransportError from None

        handler = self._router.get(method_name)
        if not handler:
            raise transport.ZRoutedMethodNotFound

        try:
            unpacked_request_iframes = (load(f) for f in iframes)
            response_frames = list(handler(unpacked_request_iframes))
        except transport.ZReqRepError:
            raise
        except Exception as e:
            logger.exception('Handler error: %r', e)
            raise transport.ZRepInternalError from None

        return [dump(f) for f in response_frames]
