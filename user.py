from collections import defaultdict
from itertools import count
import random

from main import Peer, Service, Transaction, EncKeyShare


class User(Peer):
    """Users of the blockchain. They send transactions."""

    instance_counter = count()

    def __init__(self, env, uplink, downlink, tx_spawn_interval, keyper_threshold):
        super().__init__(env, uplink, downlink)
        self.block = None
        self.tx_spawn_service = TxSpawnService(env, self, tx_spawn_interval, keyper_threshold)
        self.services.append(self.tx_spawn_service)


class TxSpawnService(Service):
    """Creates transactions at constant intervals and passes them to the distributor.

    It watches for collations to know which will be the next block.
    """

    def __init__(self, env, peer, spawn_interval, keyper_threshold):
        super().__init__(env, peer)
        self.spawn_interval = spawn_interval
        self.current_block = 0
        self.keyper_threshold = keyper_threshold

    def start(self):
        enc_watcher = self.env.process(self.watch_enc_key_shares())
        transaction_creator = self.env.process(self.create_transactions())
        yield self.env.all_of([enc_watcher, transaction_creator])

    def create_transactions(self):
        yield self.env.timeout(random.random() * self.spawn_interval)
        while True:
            # self.logger.info('sending tx', time=self.env.now)
            transaction = Transaction(self.peer.instance_number)
            self.peer.distributor.distribute(transaction)
            yield self.env.timeout(self.spawn_interval)

    def watch_enc_key_shares(self):
        enc_key_shares = set()
        enc_key_share_senders = set()
        while True:
            shares = yield self.env.process(self.peer.distributor.get_items(
                EncKeyShare.type_id,
                self.current_block,
                exclude=enc_key_shares
            ))
            enc_key_shares |= shares
            enc_key_share_senders |= set(share.sender for share in shares)
            if len(enc_key_share_senders) >= self.keyper_threshold:
                self.current_block += 1
                enc_key_shares = set()
                enc_key_share_senders = set()
