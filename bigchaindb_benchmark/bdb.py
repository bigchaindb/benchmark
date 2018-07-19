from uuid import uuid4
from itertools import count

from .utils import ts

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.exceptions import TransportError
from bigchaindb_driver.crypto import generate_keypair


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


def send(args, peer, tx):
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
