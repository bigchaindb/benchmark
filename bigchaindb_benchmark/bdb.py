import requests
from uuid import uuid4
from itertools import count
from time import sleep

from .utils import ts

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.exceptions import TransportError
from bigchaindb_driver.crypto import generate_keypair
from cachetools.func import ttl_cache
from urllib.parse import urlparse

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
            return
        yield _generate(keypair, size)

@ttl_cache(ttl=60)
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
    # Stop sending transactions if unconfirmed
    # transaction in mempool are above the set
    # threshold
    TM_HTTP_ENDPOINT = 'http://{}:26657'.format(urlparse(peer).hostname)
    unconfirmed_tx_th = args.unconfirmed_tx_th
    unconfirmed_txs = get_unconfirmed_tx(TM_HTTP_ENDPOINT)
    backoff_time = 0.5
    while unconfirmed_txs > unconfirmed_tx_th:
        sleep(backoff_time)
        unconfirmed_txs = get_unconfirmed_tx(TM_HTTP_ENDPOINT)

    driver = BigchainDB(peer, headers=args.auth)

    ts_send = ts()
    ts_error = None
    ts_accept = None

    try:
        driver.transactions.send(tx, mode=args.mode)
    except TransportError:
        ts_error = ts()
    else:
        ts_accept = ts()

    return peer, tx['id'], ts_send, ts_accept, ts_error

def sendstar(args):
    return send(*args)
