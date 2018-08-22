import base64
from json import loads, dumps
from uuid import uuid4
import logging

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair


log = logging.getLogger(__name__)


def encode(tx):
    return base64.b64encode(dumps(tx).encode('utf8')).decode('utf8')


def generate(keypair=None, size=None):
    driver = BigchainDB()

    if not keypair:
        keypair = generate_keypair()

    asset = None

    if size:
        asset = {'data': {'_': 'x' * size}}

    prepared_creation_tx = driver.transactions.prepare(
        operation='CREATE',
        signers=keypair.public_key,
        asset=asset,
        metadata={'_': str(uuid4())})

    fulfilled_creation_tx = driver.transactions.fulfill(
        prepared_creation_tx,
        private_keys=keypair.private_key)

    return fulfilled_creation_tx
