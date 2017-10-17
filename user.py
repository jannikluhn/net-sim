from collections import defaultdict
from itertools import count
import random

import config
from main import Peer, Service, Message


class NewTransactionsMessage(Message):

    def __init__(self, transactions):
        self.transactions = transactions

    @property
    def size(self):
        return sum(len(tx) for tx in self.transactions)

    def __repr__(self):
        return '<NewTxsMessage n={}>'.format(len(self.transactions))


class User(Peer):
    """Users of the blockchain.
    
    Users are the sources of transactions. They listen for encryption key shares, collations,
    decryption key shares, and votes.
    """

    instance_counter = count()
    accepted_messages = [NewTransactionsMessage]

    def __init__(self, env, uplink, downlink, tx_spawn_interval):
        super().__init__(env, uplink, downlink)
        self.block = None
        self.tx_spawn_service = TxSpawnService(
            env,
            self,
            tx_spawn_interval,
            self.tx_distribution_service
        )
        self.services.extend([self.tx_spawn_service])


class TxSpawnService(Service):
    """Creates random txs and passes them to a `TxDistributionService`."""

    def __init__(self, env, peer, spawn_interval, tx_distribution_service):
        super().__init__(env, peer)
        self.spawn_interval = spawn_interval
        self.tx_distribution_service = tx_distribution_service

    # def start(self):
    #     yield self.env.timeout(random.random() * self.spawn_interval)
    #     while True:
    #         self.logger.info('created tx', time=self.env.now)
    #         transaction = Transaction()
    #         self.peer.distributor.distribute(transaction)
    #         yield self.env.timeout(self.spawn_interval)
