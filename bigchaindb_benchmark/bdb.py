import requests
from requests.exceptions import ConnectionError, Timeout
from websocket import create_connection
import logging
import random

import base64
from json import dumps
from uuid import uuid4
from itertools import count
from time import sleep

from .utils import ts

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.exceptions import TransportError
from bigchaindb_driver.crypto import generate_keypair
from cachetools.func import ttl_cache
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def _generate(keypair=None, size=None):
    driver = BigchainDB()

    if keypair:
        alice = keypair
    else:
        alice = generate_keypair()

    asset = None

    if size:
        asset = {'data': {'_': 'x' * size}}

    prepared_creation_tx = driver.transactions.prepare(
        operation='CREATE',
        signers=alice.public_key,
        asset=asset,
        metadata={'_': str(uuid4())})

    fulfilled_creation_tx = driver.transactions.fulfill(
        prepared_creation_tx,
        private_keys=alice.private_key)

    return fulfilled_creation_tx


def generate(keypair=None, size=None, amount=None):
    for i in count():
        if i == amount:
            break
        yield _generate(keypair, size)

@ttl_cache(ttl=10)
def get_unconfirmed_tx(tm_http_api):
    num_unconfirmed_txs_api = '/num_unconfirmed_txs'
    tm_http_api = tm_http_api.strip('/')
    url = tm_http_api + num_unconfirmed_txs_api
    try:
        resp = requests.get(url)
        if resp.status_code == requests.codes.ok:
            return int(resp.json()['result']['n_txs'])
    except:
        raise

def send(peer, tx, headers={}, mode='sync'):
    driver = BigchainDB(peer, headers=headers)

    ts_send = ts()
    ts_error = None
    ts_accept = None

    try:
        driver.transactions.send(tx, mode=mode)
    except Exception as e:
        ts_error = ts()
    else:
        ts_accept = ts()
    return peer, tx['id'], len(dumps(tx)), ts_send, ts_accept, ts_error


def worker_send(args, requests_queue, results_queue):
    tries = 0
    while True:
        tx = requests_queue.get()
        result = send(random.choice(args.peer),
                      tx,
                      args.auth,
                      args.mode)
        if result[5]:
            print('Error, going to sleep for %ss', 2**tries)
            sleep(2**tries)
            tries = min(tries + 1, 4)
        else:
            tries = 0
        results_queue.put(result)


def worker_generate(args, requests_queue):
    keypair = generate_keypair()
    for tx in generate(keypair=keypair, size=args.size, amount=args.requests_per_worker):
        requests_queue.put(tx)
