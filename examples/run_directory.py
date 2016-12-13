#!/usr/bin/env python3
"""
Run directory service
"""
import argparse
import logging

from services.directory.server import DirectoryService

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main(args):
    logger.info('Run directory on address %r', args.address)
    DirectoryService(args.address).run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run directory service')
    parser.add_argument(
        'address', default='ipc:///tmp/directory',
        nargs='?',
        help='address to bind directory socket'
    )
    args = parser.parse_args()
    main(args)
