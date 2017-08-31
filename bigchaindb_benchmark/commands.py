import sys
import argparse
import logging
from functools import partial
import multiprocessing as mp

import coloredlogs

from . import utils, bdb


logger = logging.getLogger(__name__)


def run_send(args):
    send = utils.unpack(partial(bdb.send, args))

    with mp.Pool(args.processes) as pool:
        results = pool.imap_unordered(
                send,
                zip(args.peer * args.requests,
                    bdb.infinite_generate(args.broadcast,
                                          args.size)))
        for peer, txid, delta in results:
            logger.info('Send %s to %s [%.3fms]', txid, peer, delta * 1e3)


def create_parser():
    parser = argparse.ArgumentParser(
        description='Benchmarking tools for BigchainDB.')

    parser.add_argument('-l', '--log-level',
                        default='INFO')

    parser.add_argument('-p', '--peer',
                        action='append',
                        help='BigchainDB peer to use. This option can be '
                             'used multiple times.')

    parser.add_argument('-a', '--auth',
                        help='Set authentication tokens, '
                             'format: <app_id>:<app_key>).')

    parser.add_argument('--processes',
                        default=mp.cpu_count(),
                        help='Number of processes to spawn.')

    parser.add_argument('--size', '-s',
                        help='Asset size in bytes',
                        type=int,
                        default=0)

    # all the commands are contained in the subparsers object,
    # the command selected by the user will be stored in `args.command`
    # that is used by the `main` function to select which other
    # function to call.
    subparsers = parser.add_subparsers(title='Commands',
                                       dest='command')

    send_parser = subparsers.add_parser('send',
                                        help='Send a single create '
                                        'transaction from a random keypair')

    send_parser.add_argument('--requests', '-r',
                             help='Number of transactions to send to a peer.',
                             type=int,
                             default=1)

    send_parser.add_argument('--broadcast', '-b',
                             help='Broadcast the same transaction N peers. '
                                  '(Default is 1)',
                             type=int,
                             default=1)

    return parser


def configure(args):
    coloredlogs.install(level=args.log_level, logger=logger)


def main():
    utils.start(create_parser(),
                sys.argv[1:],
                globals(),
                callback_before=configure)
