from collections import defaultdict, namedtuple
from itertools import chain, count
import random
from main import Peer, Service, Message


SecretShare = namedtuple('SecretShare', ['sender', 'receiver', 'block'])
SecretShareWitness = namedtuple('SecretShareWitness', ['sender', 'block'])
Nonce = namedtuple('Nonce', ['sender', 'block'])
EncKeyShare = namedtuple('EncKeyShare', ['sender', 'block'])
# DecKeyShare = namedtuple('DecKeyShare', ['block', 'sender'])


class ThresholdEncryptionUpdates(Message):

    secret_share_size = 2 * 32 + 2 * 20  # two secret numbers, two addresses
    witness_size = 32 + 20  # witness, address
    nonce_size = 32 + 20  # nonce, address

    def __init__(self, block, secret_shares=None, witnesses=None, nonces=None):
        self.block = block
        self.secret_shares = secret_shares or set()
        self.witnesses = witnesses or set()
        self.nonces = nonces or set()

    @property
    def size(self):
        return sum([
            self.base_size,
            32,  # block number
            len(self.secret_shares) * self.secret_share_size,
            len(self.witnesses) * self.witness_size,
            len(self.nonces) + self.nonce_size
        ])


class Keyper(Peer):

    instance_counter = count()
    accepted_messages = [
        ThresholdEncryptionUpdates
    ]

    def __init__(self, env, uplink, downlink, keyper_number, threshold, ):
        super().__init__(env, uplink, downlink)
        self.threshold_encryption_service = ThresholdEncryptionService(
            env,
            self,
            keyper_number,
            threshold
        )
        self.threshold_distribution_service = ThresholdMessageDistributionService(
            env,
            self,

        )
        self.services.append(self.threshold_encryption_service)
        self.services.append(self.threshold_distribution_service)


class ThresholdEncryptionProtocol():

    def __init__(self, n, k, my_id):
        self.n = n
        self.k = k
        self.my_id = my_id
        self.secret_shares = set()
        self.witnesses = set()
        self.nonces = set()
        self.enc_key_shares = set()
        # self.keypers_with_dec_key_share = set()

    def key_distribution_finished(self):
        block = next(secret_share.block for secret_share in self.secret_shares)
        assert all(secret_share.block == block for secret_share in self.secret_shares)
        assert all(witness.block == block for witness in self.witnesses)
        assert all(secret_share.sender != self.my_id for secret_share in self.secret_shares)
        assert all(witness.sender != self.my_id for witness in self.witnesses)
        assert all(witness.receiver == self.my_id for witness in self.witnesses)
        return len(self.secret_shares) == len(self.witnesses) == self.n - 1

    def nonce_collection_finished(self):
        block = next(nonce.block for nonce in self.nonces)
        assert all(nonce.block == block for nonce in self.nonces)
        assert all(nonce.sender != self.my_id for nonce in self.nonces)
        return self.key_distribution_finished() and len(self.nonces) == self.n - 1


class ThresholdEncryptionService(Service):

    def __init__(self, env, peer, n, k):
        super().__init__(env, peer)
        self.n = n
        self.k = k
        self.id = self.peer.instance_number
        self.protocols = {}
        self.current_block = 0
        self.peer_to_known_secret_shares = {}
        self.peer_to_known_secret_share_witnesses = {}
        self.peer_to_known_nonces = {}
        self.peer_to_known_enc_key_shares = {}

    def handle_message(self, message, sender):
        if not isinstance(message, ThresholdEncryptionUpdates):
            return

        block = message.block
        if block not in self.protocols and block >= self.current_block:
            self.kickoff_protocol(block)
        protocol = self.protocols[block]

        for secret_share in message.secret_shares:
            assert secret_share.block == block
            if secret_share.receiver != self.id:
                continue
            if secret_share in protocol.secret_shares:
                continue
            protocol.secret_shares.add(secret_share)
            if protocol.key_distribution_finished():
                self.send_nonce(block)
        
        for witness in message.witnesses:
            assert witness.block == block
            if witness in protocol.witnesses:
                continue
            protocol.witnesses.add(witness)
            if protocol.key_distribution_finished():
                self.send_nonce(block)

        for nonce in message.nonces:
            assert nonce.block == block
            if nonce in protocol.nonces:
                continue
            protocol.nonces.add(nonce)
            if protocol.nonce_collection_finished():
                self.send_enc_key_share(block)

        # start protocol for next block if current one has finished
        current_protocol = self.protocols.get(self.current_block, None)
        if current_protocol and current_protocol.nonce_collection_finished():
            self.protocols.pop(self.current_block)
            self.current_block += 1
            if self.current_block not in self.protocols:
                self.kickoff_protocol(self.current_block)

    def kickoff_protocol(self, block):
        """Initialize protocol and transmit your secret shares and witnesses."""
        assert block not in self.protocols
        self.logger.info('kicking of protocol', block=block, time=self.env.now)
        protocol = ThresholdEncryptionProtocol(self.n, self.k, self.id)
        self.protocols[block] = protocol
        my_witness = SecretShareWitness(block, self.peer.instance_number)
        self.peer.threshold_distribution_service.witnesses.add(my_witness)
        for i in chain(range(0, self.id), range(self.id + 1, self.n)):
            secret_share = SecretShare(block, self.id, i)
            self.peer.threshold_distribution_service.secret_shares.add(secret_share)

    def send_nonce(self, block):
        my_nonce = Nonce(block, self.id)
        if my_nonce not in self.peer.threshold_distribution_service.nonces:
            self.logger.info('publishing nonce', block=block, time=self.env.now)
            self.peer.threshold_distribution_service.nonces.add(my_nonce)
    
    def send_enc_key_share(self, block):
        pass
        # my_enc_share = EncKeyShare(block, self.id)
        # if my_enc_share not in self.peer.threshold_distribution_service.enc_key_shares:
        #     self.logger.info('publishing encryption key share', block=block, time=self.env.now)
        #     self.peer.threshold_distribution_service.enc_key_shares.add(my_enc_share)

    def start(self):
        yield self.env.timeout(random.random() * 1)
        assert self.current_block == 0
        if self.current_block not in self.protocols:
            self.kickoff_protocol(self.current_block)


class ThresholdMessageDistributionService(Service):

    def __init__(self, env, peer):
        super().__init__(env, peer)
        self.resend_interval = 1
        self.secret_shares = set()
        self.witnesses = set()
        self.nonces = set()
        self.peer_to_known_secret_shares = defaultdict(set)
        self.peer_to_known_witnesses = defaultdict(set)
        self.peer_to_known_nonces = defaultdict(set)

    def handle_message(self, message, sender):
        if not isinstance(message, ThresholdEncryptionUpdates):
            return

        # remember received data (and that sender knows them too)
        self.secret_shares |= message.secret_shares
        self.peer_to_known_secret_shares[sender] |= message.secret_shares
        self.witnesses |= message.witnesses
        self.peer_to_known_witnesses[sender] |= message.witnesses
        self.nonces |= message.nonces
        self.peer_to_known_nonces[sender] |= message.nonces

        # forget data that all my peers know about
        secret_shares = list(self.peer_to_known_secret_shares.values())
        common_secret_shares = set.intersection(*secret_shares or [set()])
        self.secret_shares.difference_update(common_secret_shares)
        for known_by_peer in self.peer_to_known_secret_shares.values():
            known_by_peer.difference_update(common_secret_shares)
        witnesses = list(self.peer_to_known_witnesses.values())
        common_witnesses = set.intersection(*witnesses or [set()])
        self.witnesses.difference_update(common_witnesses)
        for known_by_peer in self.peer_to_known_witnesses.values():
            known_by_peer.difference_update(common_witnesses)
        nonces = list(self.peer_to_known_nonces.values())
        common_nonces = set.intersection(*nonces or [set()])
        self.nonces.difference_update(common_nonces)
        for known_by_peer in self.peer_to_known_nonces.values():
            known_by_peer.difference_update(common_nonces)

    def start(self):
        # make sure not everyone starts at same time
        yield self.env.timeout(random.random() * self.resend_interval)
        while True:
            for other in self.peer.peers:
                new_secret_shares = self.secret_shares - self.peer_to_known_secret_shares[other]
                new_witnesses = self.witnesses - self.peer_to_known_witnesses[other]
                new_nonces = self.nonces - self.peer_to_known_nonces[other]
                if new_secret_shares or new_witnesses or new_nonces:
                    blocks = [d.block for d in
                              set.union(new_secret_shares, new_witnesses, new_nonces)]
                    for block in blocks:
                        message = ThresholdEncryptionUpdates(
                            block,
                            set(d for d in new_secret_shares if d.block == block),
                            set(d for d in new_witnesses if d.block == block),
                            set(d for d in new_nonces if d.block == block))
                        self.env.process(self.peer.send(message, other))
            yield self.env.timeout(self.resend_interval)
