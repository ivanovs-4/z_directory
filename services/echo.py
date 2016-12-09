"""
"""
from services import directory
from z_arch import ZService


class EchoService(ZService):
    code = 'echo'
    # client = EchoClient

    def __init__(self, address):
        self._address = address

    def about(self):
        return {
            'code': self.code,
            'info': 'yes',
        }

    def run(self, directory_address):
        # 1. connect to directory, say who we are
        _dir = directory.client.Directory(directory_address)
        node = _dir.register(self)
        print(node)

        """
        2. bind aur service socket and reply to it,
        meantime send heartbeat to directory
        """
