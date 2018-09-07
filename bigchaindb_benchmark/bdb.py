import requests
from requests.exceptions import ConnectionError, Timeout
from websocket import create_connection
import logging

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

def send(args, peer, tx):
    driver = BigchainDB(peer, headers=args.auth)

    ts_send = ts()
    ts_error = None
    ts_accept = None

    try:
        driver.transactions.send(tx, mode=args.mode)
    except Exception as e:
        ts_error = ts()
    else:
        ts_accept = ts()
    return peer, tx['id'], len(dumps(tx)), ts_send, ts_accept, ts_error

def worker(queue, args, index):
    keypair = generate_keypair()
    tries = 0
    for tx in generate(keypair=keypair, size=args.size, amount=args.requests_per_worker):
        result = send(args, args.peer[index], tx)
        queue.put(result)
        if result[5]:
            logger.info('Error, going to sleep for %ss', 2**tries)
            sleep(2**tries)
            tries = min(tries + 1, 4)
        else:
            tries = 0
