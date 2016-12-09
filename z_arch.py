from transport import ReqRepMsgpackTransport


class ZClient:
    def __init__(self, address):
        self._address = address

    @classmethod
    def construct_from_about(cls, about):
        return cls(about[0]['rep_address'])

    def query_raw(self, *args):
        tran = ReqRepMsgpackTransport(self._address)
        return tran.req(*args)


class ZService:
    client = ZClient
