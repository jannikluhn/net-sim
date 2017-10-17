from collections import defaultdict, namedtuple
from itertools import chain, count
import random
from main import Peer, Service, Message, SecretShare, Witness, Nonce


class Keyper(Peer):

    instance_counter = count()
    keyper_ids = []

    def __init__(self, env, uplink, downlink, threshold):
        super().__init__(env, uplink, downlink)
        self.keyper_ids.append(self.instance_number)
        self.threshold_encryption_service = ThresholdEncryptionService(env, self, threshold)
        self.services.append(self.threshold_encryption_service)


class ThresholdEncryptionProtocol():

    def __init__(self, k, all_ids, my_id):
        self.n = len(all_ids)
        self.k = k
        self.my_id = my_id
        self.all_ids = set(all_ids)
        self.other_ids = self.all_ids - set([self.my_id])

        self.secret_shares = set()
        self.witnesses = set()
        self.nonces = set()
        self.enc_key_shares = set()
        # self.keypers_with_dec_key_share = set()

    def key_distribution_finished(self):
        # all messages should be for the same block
        if not self.secret_shares:
            return False
        block = next(secret_share.block for secret_share in self.secret_shares)
        assert all(secret_share.block == block for secret_share in self.secret_shares)
        assert all(witness.block == block for witness in self.witnesses)
        # secret shares should be addressed to us
        assert all(secret_share.receiver == self.my_id for secret_share in self.secret_shares)
        # key distribution is finished as soon as secret 
        secret_share_senders = set(secret_share.sender for secret_share in self.secret_shares)
        witness_senders = set(witness.sender for witness in self.witnesses)
        assert self.my_id not in secret_share_senders
        assert self.my_id not in witness_senders
        return secret_share_senders == witness_senders == self.other_ids

    def nonce_collection_finished(self):
        block = next((nonce.block for nonce in self.nonces), None)
        assert all(nonce.block == block for nonce in self.nonces)
        nonce_senders = set(nonce.sender for nonce in self.nonces)
        assert self.my_id not in nonce_senders
        if not self.key_distribution_finished():
            return False
        return set(nonce.sender for nonce in self.nonces) == self.other_ids


class ThresholdEncryptionService(Service):

    def __init__(self, env, peer, k):
        super().__init__(env, peer)
        self.k = k
        self.all_ids = None  # will be set once all keypers have been initialized (in `start`)
        self.my_id = self.peer.instance_number
        self.protocols_by_block = {}
        self.current_block = 0

    def start(self):
        self.all_ids = set(self.peer.keyper_ids)  # now all keypers are initialized
        while True:
            # start with next protocol
            self.logger.info('kicking of protocol', block=self.current_block, time=self.env.now)
            protocol = ThresholdEncryptionProtocol(self.k, self.all_ids, self.my_id)
            self.protocols_by_block[self.current_block] = protocol
            assert self.current_protocol is protocol

            self.send_secrets()
            yield self.env.process(self.collect_secrets())
            self.send_nonce()
            yield self.env.process(self.collect_nonces())

            self.current_block += 1

    def send_secrets(self):
        """Send secret shares and witness."""
        self.logger.info('sending secrets', block=self.current_block, time=self.env.now)
        assert not self.current_protocol.key_distribution_finished()
        for other_id in self.all_ids:
            if other_id == self.my_id:
                continue
            secret_share = SecretShare(self.current_block, self.my_id, other_id)
            self.peer.distributor.distribute(secret_share)
        witness = Witness(self.current_block, self.my_id)
        self.peer.distributor.distribute(witness)

    def collect_secrets(self):
        """Collect secret shares and witness from all other keypers."""
        self.logger.info('waiting for secrets', block=self.current_block, time=self.env.now)
        assert not self.current_protocol.key_distribution_finished()
        processed_items = set()
        # collect all secret shares and witnesses
        while not self.current_protocol.key_distribution_finished():
            items = yield self.env.process(self.peer.distributor.get_items(
                (SecretShare.type_id, Witness.type_id),
                self.current_block,
                exclude=processed_items
            ))
            secret_shares = set(item for item in items
                                if isinstance(item, SecretShare) and item.receiver == self.my_id)
            witnesses = set(item for item in items
                            if isinstance(item, Witness) and item.sender != self.my_id)
            self.current_protocol.secret_shares |= secret_shares
            self.current_protocol.witnesses |= witnesses
            processed_items |= items

    def send_nonce(self):
        """Send my nonce to other keypers."""
        self.logger.info('sending nonce', block=self.current_block, time=self.env.now)
        assert not self.current_protocol.nonce_collection_finished()
        nonce = Nonce(self.current_block, self.my_id)
        self.peer.distributor.distribute(nonce)

    def collect_nonces(self):
        """Collect nonces from all other keypers"""
        self.logger.info('waiting for nonces', block=self.current_block, time=self.env.now)
        assert not self.current_protocol.nonce_collection_finished()
        processed_nonces = set()
        while not self.current_protocol.nonce_collection_finished():
            items = yield self.env.process(self.peer.distributor.get_items(
                Nonce.type_id,
                self.current_block,
                exclude=processed_nonces
            ))
            nonces = set(item for item in items if item.sender != self.my_id)
            self.current_protocol.nonces |= nonces
            processed_nonces |= items

    @property
    def current_protocol(self):
        return self.protocols_by_block.get(self.current_block, None)
