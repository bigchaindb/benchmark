import logging
import multiprocessing as mp

from .transaction import encode, generate


log = logging.getLogger(__name__)


def worker(queue):
    while True:
        queue.put(encode(generate()))


def start(processes=mp.cpu_count()):
    queue = mp.Queue(10000)
    for _ in range(processes):
        p = mp.Process(target=worker, args=(queue,))
        p.start()

    log.info('Started %s transaction generators',
             processes)

    return queue
