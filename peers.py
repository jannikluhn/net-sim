from collections import defaultdict
from itertools import count
import math
import random

import structlog

import config
from messages import NewTransactionsMessage, CollationMessage


class Peer(object):

    instance_counter = count()
    accepted_messages = []

    def __init__(self, env, uplink, downlink):
        self.env = env
        self.services = []
        self.instance_number = next(self.instance_counter)

        self.logger = structlog.get_logger(self.__class__.__name__ + str(self.instance_number))
        self.logger = self.logger.bind(node=self)

        self.uplink = uplink
        self.downlink = downlink

        self.uplink_allocations = []  # [(bandwidth, used until), ...]
        self.downlink_allocations = []  # [(bandwidth, used until), ...]

        self.peers = []

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.instance_number)

    def start(self):
        for service in self.services:
            self.env.process(service.start())

    def send(self, message, receiver):
        # don't send message if receiver isn't interested
        if not any(isinstance(message, MessageType) for MessageType in receiver.accepted_messages):
            return
        transmitted = 0
        not_transmitted = message.size
        time = self.env.now

        uplink_allocations = iter(self.uplink_allocations)
        downlink_allocations = iter(receiver.downlink_allocations)
        new_uplink_allocations = []
        new_downlink_allocations = []

        # skip historic allocations
        uplink_used_until = -math.inf
        while uplink_used_until < self.env.now:
            uplink_used, uplink_used_until = next(uplink_allocations, (0, math.inf))
        downlink_used_until = -math.inf
        while downlink_used_until < self.env.now:
            downlink_used, downlink_used_until = next(downlink_allocations, (0, math.inf))

        while True:
            # check how much bandwidth is available
            if time >= uplink_used_until:
                uplink_used, uplink_used_until = next(uplink_allocations, (0, math.inf))
            if time >= downlink_used_until:
                downlink_used, downlink_used_until = next(uplink_allocations, (0, math.inf))

            # allocate bandwidth
            bandwidth = min(self.uplink - uplink_used, receiver.downlink - downlink_used)
            expected_transmission_time = float(not_transmitted) / bandwidth
            bandwidth_used_until = min([
                uplink_used_until,
                downlink_used_until,
                time + expected_transmission_time
            ])
            new_uplink_allocations.append((uplink_used + bandwidth, bandwidth_used_until))
            new_downlink_allocations.append((downlink_used + bandwidth, bandwidth_used_until))

            # calculate transmission time
            transmitted += bandwidth * (bandwidth_used_until - time)
            not_transmitted = message.size - transmitted
            time = bandwidth_used_until
            if math.isclose(not_transmitted, 0):
                break

        # replay other allocations
        new_uplink_allocations.append((uplink_used, uplink_used_until))
        new_downlink_allocations.append((downlink_used, downlink_used_until))
        for uplink_used, uplink_used_until in uplink_allocations:
            new_uplink_allocations.append((uplink_used, uplink_used_until))
        for downlink_used, downlink_used_until in downlink_allocations:
            new_downlink_allocations.append((downlink_used, downlink_used_until))
        self.uplink_allocations = new_uplink_allocations
        receiver.downlink_allocations = new_downlink_allocations

        # sleep and receive
        yield self.env.timeout(time - self.env.now)
        receiver.receive(message, self)

    def receive(self, message, sender):
        for service in self.services:
            service.handle_message(message, sender)

    def connect(self, peer):
        if peer is self:
            return
        if peer not in self.peers:
            self.logger.info('connecting', to=peer, time=self.env.now)
            self.peers.append(peer)
            peer.connect(self)


class Service(object):

    def __init__(self, env, peer):
        self.env = env
        self.peer = peer
        self.logger = self.peer.logger.bind(service=self.__class__.__name__)

    def handle_message(self, message, sender):
        pass

    def start(self):
        pass


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


class CollationService(Service):

    def __init__(self, env, peer, collation_interval):
        super().__init__(env, peer)
        self.txs = set()
        self.n_txs = 0
        self.n_colls = 0
        self.collation_interval = collation_interval
    
    def start(self):
        while True:
            yield self.env.timeout(self.collation_interval)
            txs = list(self.txs)
            self.txs.clear()
            message = CollationMessage(txs)
            self.n_colls += 1
            self.logger.info('created collation', n=self.n_colls, txs=len(txs), time=self.env.now)
            for other in self.peer.peers:
                self.peer.send(message, other)

    def handle_message(self, message, sender):
        if isinstance(message, NewTransactionsMessage):
            n_txs_before = len(self.txs)
            self.txs.update(message.transactions)
            n_txs_after = len(self.txs)
            n_new = n_txs_after - n_txs_before
            self.n_txs += n_new
            self.logger.debug('received txs',
                peer=sender,
                n=len(message.transactions),
                known=len(message.transactions) - n_new,
                total=self.n_txs,
                time=self.env.now
            )


class Collator(Peer):

    instance_counter = count()
    accepted_messages = [NewTransactionsMessage]

    def __init__(self, env, uplink, downlink, collation_interval):
        super().__init__(env, uplink, downlink)
        self.collation_interval = collation_interval
        self.collation_service = CollationService(env, self, collation_interval)
        self.services.append(self.collation_service)
