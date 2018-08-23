import sys
import argparse
import asyncio
import logging
import multiprocessing as mp

import coloredlogs

from . import utils


log = logging.getLogger(__name__)


def init(args):
    coloredlogs.install(level=args.log_level)

    def emit(stats):
        log.info('Processing transactions,'
            'queue: %s (%s tx/s), sent: %s (%s tx/s), accepted: %s (%s tx/s), committed %s (%s tx/s), errored %s (%s tx/s), mempool %s (%s tx/s)',
            stats['queue'], stats.get('queue.speed', 0),
            stats['sent'], stats.get('sent.speed', 0),
            stats['accept'], stats.get('accept.speed', 0),
            stats['commit'], stats.get('commit.speed', 0),
            stats['error'], stats.get('error.speed', 0),
            stats['mempool'], stats.get('mempool.speed', 0))

    import logstats

    ls = logstats.Logstats(emit_func=emit)
    ls['accept'] = 0
    ls['commit'] = 0
    ls['error'] = 0

    logstats.thread.start(ls)

    args.ls = ls


def create_parser():
    parser = argparse.ArgumentParser(
        description='Benchmarking tools for BigchainDB.')

    parser.add_argument('--csv',
                        type=str,
                        default='out.csv')

    parser.add_argument('-l', '--log-level',
                        default='INFO')

    parser.add_argument('-p', '--peer',
                        action='append',
                        help='BigchainDB peer to use. This option can be '
                             'used multiple times.')

    parser.add_argument('--host',
                        type=str,
                        default='localhost')

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
                             default='sync')

    send_parser.add_argument('--rate', '-r',
                             help='Number of transactions to send to a peer.',
                             type=float,
                             default=1)

    return parser


def run_send(args):
    from . import sender
    from . import generator

    queue = generator.start(args.processes)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sender.start(args, queue))
    loop.run_forever()


def main():
    utils.start(create_parser(),
                sys.argv[1:],
                globals(),
                callback_before=init)
