import sys
import argparse
import logging
from functools import partial
import multiprocessing as mp

import coloredlogs

from . import utils, bdb


logger = logging.getLogger(__name__)


def run_check(args, keypair):
    from pymongo import MongoClient
    import time
    from collections import Counter

    counters = Counter()
    diffs = Counter()

    while True:
        for peer in args.peer_mdb:
            client = MongoClient('mongodb://' + peer)
            db = client['bigchain']
            count = db['backlog'].find({'outputs.public_keys': keypair.public_key}).count()
            diffs[peer] = count - counters[peer]
            counters[peer] = count
            logger.info('Found %s transactions in %s [%s]', count, peer, diffs[peer])

        if not(any(diffs.values())):
            break
        time.sleep(0.5)

    for peer in args.peer_mdb:
        if counters[peer] != args.requests * len(args.peer):
            logger.error('Peer %s got an unexpected number of transactions. '
                         '%s != %s', peer, counters[peer], args.requests)
        else:
            logger.info('Peer %s got expected %s transactions.', peer, counters[peer])


def run_send(args):
    from bigchaindb_driver.crypto import generate_keypair

    keypair = generate_keypair()
    send = utils.unpack(partial(bdb.send, args))
    generator = partial(bdb.infinite_generate, keypair, args.broadcast, args.size)

    with mp.Pool(args.processes) as pool:
        results = pool.imap_unordered(
                send,
                zip(args.peer_bdb * args.requests, generator()))
        for peer, txid, delta in results:
            logger.info('Send %s to %s [%.3fms]', txid, peer, delta * 1e3)

    if args.check:
        run_check(args, keypair)


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

    send_parser.add_argument('--check', '-c',
                             help='Check the transactions against the DB.'
                                  '(Requires full access to MongoDB)',
                             action='store_true',
                             default=False)

    return parser


def configure(args):
    coloredlogs.install(level=args.log_level, logger=logger)


def main():
    utils.start(create_parser(),
                sys.argv[1:],
                globals(),
                callback_before=configure)
