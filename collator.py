from itertools import count
from main import Message, Peer, Service, Collation, Transaction


class Collator(Peer):

    instance_counter = count()

    def __init__(self, env, uplink, downlink, collation_interval):
        super().__init__(env, uplink, downlink)
        self.collation_service = CollationService(env, self, collation_interval)
        self.services.append(self.collation_service)


class CollationService(Service):

    def __init__(self, env, peer, collation_interval):
        super().__init__(env, peer)
        self.collation_interval = collation_interval
        self.next_collation_block = 0
        self.collation_size = 0

    def start(self):
        transaction_watcher = self.env.process(self.watch_transactions())
        collation_creator = self.env.process(self.create_collations())
        yield self.env.all_of([transaction_watcher, collation_creator])

    def watch_transactions(self):
        current_block = self.next_collation_block
        processed = set()
        while True:
            if current_block != self.next_collation_block:
                processed = set()
                self.collation_size = 0
                current_block += 1
            transactions = yield self.env.process(self.peer.distributor.get_items(
                Transaction.type_id,
                current_block,
                exclude=processed
            ))
            self.collation_size += len(transactions - processed)
            processed |= transactions

    def create_collations(self):
        while True:
            yield self.env.timeout(self.collation_interval)
            self.logger.info(
                'creating collation',
                block=self.next_collation_block,
                txs=self.collation_size,
                time=self.env.now
            )
            collation = Collation(self.next_collation_block, self.peer.instance_number)
            self.peer.distributor.distribute(collation)
            self.next_collation_block += 1
