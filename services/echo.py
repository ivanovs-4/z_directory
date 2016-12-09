"""
"""
import logging

from services import directory
from z_arch import ZService

logger = logging.getLogger(__name__)


class EchoService(ZService):
    code = b'echo'
    # client = EchoClient

    def __init__(self, address):
        self._address = address

    def run(self, directory_address):
        # 1. connect to directory, say who we are
        _dir = directory.client.DirectoryClient(directory_address)
        node = _dir.register(self)
        logger.info('Node: %r', node)

        """
        2. bind aur service socket and reply to it,
        meantime send heartbeat to directory
        """
        while True:
            pass

    def about(self):
        return {
            b'code': self.code,
            b'rep_address': self._address,
        }
