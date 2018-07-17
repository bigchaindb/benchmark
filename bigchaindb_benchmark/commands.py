import sys
import argparse
import logging
from functools import partial
from itertools import cycle, repeat
from threading import Thread
import json
import multiprocessing as mp

import coloredlogs
from websocket import create_connection

import bigchaindb_benchmark

from . import utils, bdb


logger = logging.getLogger(__name__)


def run_send(args):
    from bigchaindb_driver.crypto import generate_keypair
    from urllib.parse import urlparse

    ls = bigchaindb_benchmark.config['ls']

    keypair = generate_keypair()

    BDB_ENDPOINT = args.peer[0]
    WS_ENDPOINT = 'ws://{}:9985/api/v1/streams/valid_transactions'.format(urlparse(BDB_ENDPOINT).hostname)
    sent_transactions = []

    def listen():
        logger.info('Connecting to WebSocket %s', WS_ENDPOINT)
        ws = create_connection(WS_ENDPOINT)
        while True:
            result = ws.recv()
            transaction_id = json.loads(result)['transaction_id']
            try:
                sent_transactions.remove(transaction_id)
                ls['commit'] += 1
            except ValueError:
                pass
            if not sent_transactions:
                return

    t = Thread(target=listen, daemon=False)
    t.start()

    with mp.Pool(args.processes) as pool:
        results = pool.imap_unordered(
                bdb.sendstar,
                zip(repeat(args),
                    cycle(args.peer),
                    bdb.generate(keypair, args.size, args.requests)))
        for peer, txid, delta in results:
            sent_transactions.append(txid)
            ls['sent'] += 1
            logger.debug('Send %s to %s [%.3fms]', txid, peer, delta * 1e3)

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
                        type=int,
                        help='Number of processes to spawn.')

    # all the commands are contained in the subparsers object,
    # the command selected by the user will be stored in `args.command`
    # that is used by the `main` function to select which other
    # function to call.
    subparsers = parser.add_subparsers(title='Commands',
                                       dest='command')

    send_parser = subparsers.add_parser('send',
                                        help='Send a single create '
                                        'transaction from a random keypair')

    send_parser.add_argument('--size', '-s',
                             help='Asset size in bytes',
                             type=int,
                             default=0)

    send_parser.add_argument('--mode', '-m',
                             help='Sending mode',
                             choices=['sync', 'async', 'commit'],
                             default='async')

    send_parser.add_argument('--requests', '-r',
                             help='Number of transactions to send to a peer.',
                             type=int,
                             default=1)

    return parser


def configure(args):
    coloredlogs.install(level=args.log_level, logger=logger)

    import logstats
    ls = logstats.Logstats(logger=logger)
    logstats.thread.start(ls)
    bigchaindb_benchmark.config = {'ls': ls}


def main():
    utils.start(create_parser(),
                sys.argv[1:],
                globals(),
                callback_before=configure)
