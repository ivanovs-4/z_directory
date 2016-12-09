from transport import ReqRepMsgpackTransport


class ZClient:
    def __init__(self, info):
        self._address = info[0]['rep_address']

    def query_raw(self, *args):
        tran = ReqRepMsgpackTransport(self._address)
        return tran.req(*args)


class ZService:
    client = ZClient
