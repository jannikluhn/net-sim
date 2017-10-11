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

    instance_counter = count()
    accepted_messages = [NewTransactionsMessage]

    def __init__(self, env, uplink, downlink, tx_spawn_interval):
        super().__init__(env, uplink, downlink)
        self.tx_distribution_service = TxDistributionService(env, self)
        self.tx_spawn_service = TxSpawnService(
            env,
            self,
            tx_spawn_interval,
            self.tx_distribution_service
        )
        self.services.extend([self.tx_distribution_service, self.tx_spawn_service])


class TxDistributionService(Service):
    """Watches for new transactions and distributes them to a node's peers."""

    def __init__(self, env, peer):
        super().__init__(env, peer)
        self.known_txs = set()
        self.peer_to_known_txs = defaultdict(set)
        self.resend_interval = 1
        self.n_txs = 0

    def start(self):
        # make sure not everyone starts at same time
        yield self.env.timeout(random.random() * self.resend_interval)
        while True:
            for other in self.peer.peers:
                # send txs we know but peer does not
                new_txs = self.known_txs - self.peer_to_known_txs[other]
                if len(new_txs) > 0:
                    message = NewTransactionsMessage(new_txs)
                    self.peer.send(message, other)
                    # now peer knows them as well
                    self.peer_to_known_txs[other].update(new_txs)
            # forget messages that everyone knows about
            txs_to_drop = set.intersection(*list(self.peer_to_known_txs.values()) or [set()])
            self.known_txs.intersection_update(txs_to_drop)
            for known_by_peer in self.peer_to_known_txs.values():
                known_by_peer.intersection_update(txs_to_drop)
            yield self.env.timeout(self.resend_interval)


    def handle_message(self, message, sender):
        if isinstance(message, NewTransactionsMessage):
            # remember txs, and note that peer knows them too
            n_txs_before = len(self.known_txs)
            self.known_txs.update(message.transactions)
            n_txs_after = len(self.known_txs)
            n_new = n_txs_after - n_txs_before
            self.n_txs += n_new
            self.peer_to_known_txs[sender].update(message.transactions)
            self.logger.info('received txs',
                n=len(message.transactions),
                known=len(message.transactions) - n_new,
                total=self.n_txs,
                time=self.env.now,
                **{'from': sender}
            )

    def add_tx(self, tx):
        self.known_txs.add(tx)
        self.n_txs += 1


class TxSpawnService(Service):
    """Creates random txs and passes them to a `TxDistributionService`."""

    def __init__(self, env, peer, spawn_interval, tx_distribution_service):
        super().__init__(env, peer)
        self.spawn_interval = spawn_interval
        self.tx_distribution_service = tx_distribution_service

    def start(self):
        yield self.env.timeout(random.random() * self.spawn_interval)
        while True:
            self.logger.info('created tx', time=self.env.now)
            tx = ''.join(chr(random.randint(0, 255)) for _ in range(config.TX_SIZE))
            self.tx_distribution_service.known_txs.add(tx)
            yield self.env.timeout(self.spawn_interval)
