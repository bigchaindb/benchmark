import time
import uuid


from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair
from bigchaindb_driver.exceptions import NotFoundError


def generate_create(signer, size=None):
    driver = BigchainDB()
    asset = None

    if size:
        asset = {'data': {'_': 'x' * size}}

    metadata = {'nounce': str(uuid.uuid4())}

    prepared_creation_tx = driver.transactions.prepare(
        operation='CREATE',
        signers=signer.public_key,
        asset=asset,
        metadata=metadata)

    fulfilled_creation_tx = driver.transactions.fulfill(
        prepared_creation_tx,
        private_keys=signer.private_key)

    return fulfilled_creation_tx


def generate_transfer(prev, signer, size=None):
    driver = BigchainDB()
    asset = None

    if prev['operation'] == 'CREATE':
        asset = {
            'id': prev['id']
        }
    elif prev['operation'] == 'TRANSFER':
        asset = {
            'id': prev['asset']['id']
        }

    output_index = 0
    output = prev['outputs'][output_index]

    transfer_input = {
        'fulfillment': output['condition']['details'],
        'fulfills': {
            'output_index': output_index,
            'transaction_id': prev['id']
        },
        'owners_before': output['public_keys']
    }

    prepared_transfer_tx = driver.transactions.prepare(
        operation='TRANSFER',
        asset=asset,
        inputs=transfer_input,
        recipients=signer.public_key,
    )

    fulfilled_transfer_tx = driver.transactions.fulfill(
        prepared_transfer_tx,
        private_keys=signer.private_key,
    )

    return fulfilled_transfer_tx


def check_status(transaction_id):
    driver = BigchainDB()

    trys = 0
    status = None
    while trys < 60:
        try:
            status = driver.transactions.status(transaction_id).get('status') == 'valid'
            if status:
                break
        except NotFoundError:
            time.sleep(1)
            check_status(transaction_id)
    return status


def infinite_generate(repeat=1, size=None, consecutive=False):
    signer = generate_keypair()
    tx = None

    while True:
        if consecutive:
            if tx is None:
                tx = generate_create(signer, size)
            else:

                status = check_status(tx['id'])
                if status:
                    tx = generate_transfer(tx, signer, size)

        else:
            tx = generate_create(signer, size)

        for _ in range(repeat):
            yield tx


def send(args, peer, tx):
    driver = BigchainDB(peer, headers=args.auth)

    start = time.perf_counter()
    driver.transactions.send(tx)
    delta = time.perf_counter() - start

    return peer, tx['id'], delta
