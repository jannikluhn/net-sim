from itertools import count
from main import Message, Peer, Service
from user import NewTransactionsMessage


class CollationMessage(Message):

    def __init__(self, transactions):
        self.transactions = transactions

    @property
    def size(self):
        return self.base_size + sum(len(tx) for tx in self.transactions)

    def __repr__(self):
        return '<CollationMessage n={}>'.format(len(self.transactions))


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
