"""
"""
import itertools
import logging

from z_arch import ZReqRepService, ReqRepInterface

logger = logging.getLogger(__name__)


echo_counter = itertools.count()


class EchoInterface(ReqRepInterface):
    def reply(self, service, frames):
        return [next(echo_counter), *frames]


class EchoService(ZReqRepService):
    code = b'echo'
    # client = EchoClient
    interface = EchoInterface()
