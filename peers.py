from collections import defaultdict
from itertools import count
import structlog
import random
import simpy
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

        self.uplink = simpy.Container(self.env, init=uplink)
        self.downlink = simpy.Container(self.env, init=downlink)
        self.send_processes = []
        self.receive_processes = []

        self.peers = []

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.instance_number)

    def start(self):
        for service in self.services:
            self.env.process(service.start())

    def send(self, message, receiver):
        if not any(isinstance(message, MessageType) for MessageType in receiver.accepted_messages):
            return  # don't send message if receiver isn't interested
        def _send():
            transmitted = 0
            complete = False
            while not complete:
                # find available bandwidth (can be zero)
                bandwidth = min(self.uplink.level, receiver.downlink.level)
                # consume bandwidth
                if bandwidth > 0:
                    self.uplink.get(bandwidth)
                    receiver.downlink.get(bandwidth)
                # send message
                start = self.env.now
                try:
                    to_transmit = float(message.size) - transmitted
                    time = to_transmit / bandwidth if bandwidth != 0 else float('inf')
                    yield self.env.timeout(time)
                except simpy.Interrupt:
                    # interrupted because other process finished, redistributing bandwidth
                    pass
                else:
                    complete = True
                transmitted += bandwidth * (self.env.now - start)
                if bandwidth > 0:
                    self.uplink.put(bandwidth)
                    receiver.downlink.put(bandwidth)
            # transfer finished, interrupt all other transfers to redistribute bandwidth
            self.send_processes.remove(proc)
            receiver.receive_processes.remove(proc)
            for process in self.send_processes + receiver.receive_processes:
                process.interrupt()
            receiver.receive(message, self)
        proc = self.env.process(_send())
        self.send_processes.append(proc)
        receiver.receive_processes.append(proc)

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
