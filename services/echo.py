"""
"""
import itertools
import logging

from z_arch import ZReqRepService

logger = logging.getLogger(__name__)


echo_counter = itertools.count()


class EchoService(ZReqRepService):
    code = b'echo'
    # client = EchoClient

    def _handle(self, frames):
        logger.debug('Echo._handle frames: %r', frames)
        return [
            self._dump(next(echo_counter)),
            *frames
        ]
