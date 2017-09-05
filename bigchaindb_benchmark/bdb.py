import time
from uuid import uuid4


from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair


def generate(keypair=None, size=None):
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


def infinite_generate(keypair=None, repeat=1, size=None):
    while True:
        tx = generate(keypair, size)
        for _ in range(repeat):
            yield tx


def send(args, peer, tx):
    driver = BigchainDB(peer, headers=args.auth)

    start = time.perf_counter()
    driver.transactions.send(tx)
    delta = time.perf_counter() - start

    return peer, tx['id'], delta
