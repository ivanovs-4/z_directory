from transport import ReqRepMsgpackTransport


class ZClient:
    def __init__(self, directory, address):
        self._dir = directory
        self._address = address

    @classmethod
    def construct_from_about(cls, self, about):
        return cls(self, about[0]['rep_address'])

    def query_raw(self, *args):
        tran = ReqRepMsgpackTransport(self._address)
        return tran.req(*args)


class ZService:
    client = ZClient

    def about(self):
        return {
            'code': self.code,
            'info': 'yes',
        }
