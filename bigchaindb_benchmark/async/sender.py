import base64
from time import perf_counter
from json import loads, dumps
from uuid import uuid4
import asyncio
import logging
import multiprocessing as mp

import aiohttp
import coloredlogs
import websockets

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.exceptions import TransportError
from bigchaindb_driver.crypto import generate_keypair

from . import utils

log = logging.getLogger(__name__)


class WebSocketSender:
    def __init__(self, args, queue):
        self.ls = args.ls
        self.host = args.host
        self.queue = queue
        self.rate_delta = 1 / args.rate

    async def start(self):
        self.ws = await websockets.connect(f'ws://{self.host}:26657/websocket')
        log.info('Connection established')
        await asyncio.wait([
            self.ping(),
            self.read_mempool(),
            self.read(),
            self.write()
        ])

    async def ping(self):
        while True:
            await asyncio.sleep(10)
            await self.ws.ping()
            log.debug('Ping')

    async def read_mempool(self):
        async with aiohttp.ClientSession() as session:
            while True:
                await asyncio.sleep(1)
                async with session.get(f'http://{self.host}:26657/num_unconfirmed_txs') as response:
                    self.ls['mempool'] = int(await response.json()['result']['n_txs'])

    async def read(self):
        while True:
            try:
                message = loads(await self.ws.recv())
                log.debug('Got answer for %s', message['id'])
                self.ls['accept'] += 1
            except websockets.exceptions.ConnectionClosed:
                log.warning('Connection Closed')
                break

    async def write(self):
        jitter = 0
        while True:
            counter = perf_counter() + jitter
            _id = str(uuid4())
            tx = self.queue.get()
            message = {
                'method': 'broadcast_tx_async',
                'jsonrpc': '2.0',
                'params': [tx],
                'id': _id
            }
            try:
                await self.ws.send(dumps(message))
            except websockets.exceptions.ConnectionClosed:
                log.warning('Connection Closed')
                break
            self.ls['sent'] += 1
            self.ls['queue'] = self.queue.qsize()
            log.debug('Sent message %s', _id)
            delta = self.rate_delta - (perf_counter() - counter)

            counter = perf_counter()
            if delta > 0:
                await asyncio.sleep(delta)
            jitter = delta - (perf_counter() - counter)


async def start(args, queue):
    sender = WebSocketSender(args, queue)
    await sender.start()
