from transport import ReqRepMethodMsgpackTransport


class ZClient:
    def __init__(self, directory, address):
        self._dir = directory
        self._address = address

    @classmethod
    def construct_from_s_info(cls, self, s_info):
        # TODO use Directory protocol, methods
        first_frame = s_info[0]
        nodes_by_id = first_frame
        about = list(nodes_by_id.values())[0]
        return cls(self, about[b'rep_address'])

    def query_raw(self, *args):
        transport = ReqRepMethodMsgpackTransport(self._address)
        return transport.req(*args)


class ZService:
    client = ZClient

    def run(self, directory_address):
        raise NotImplementedError

    def about(self):
        # TODO use Directory protocol, methods
        return {
            b'code': self.code,
        }
