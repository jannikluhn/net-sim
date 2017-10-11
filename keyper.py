from collections import defaultdict, namedtuple
from itertools import chain, count
import random
from main import Peer, Service, Message


SecretShare = namedtuple('SecretShare', ['block', 'sender', 'receiver'])
SecretShareWitness = namedtuple('SecretShareWitness', ['block', 'sender'])
Nonce = namedtuple('Nonce', ['block', 'sender'])
EncKeyShare = namedtuple('EncKeyShare', ['block', 'sender'])
# DecKeyShare = namedtuple('DecKeyShare', ['block', 'sender'])


class SecretSharesMessage(Message):

    def __init__(self, secret_shares):
        self.secret_shares = secret_shares

    @property
    def size(self):
        # two 256 bit values, two addresses, block number
        return self.base_size + (2 * 32 + 2 * 20 + 32) * len(self.secret_shares)


class SecretShareWitnessesMessage(Message):

    def __init__(self, secret_share_witnesses):
        self.secret_share_witnesses = secret_share_witnesses

    @property
    def size(self):
        # one 256 bit value, one address, block number
        return self.base_size + (32 + 20 + 32) * len(self.secret_share_witnesses)


class NoncesMessage(Message):

    def __init__(self, nonces):
        self.nonces = nonces

    @property
    def size(self):
        # nonce, address, block number
        return self.base_size + (32 + 20 + 32) * len(self.nonces)


class EncKeySharesMessage(Message):

    def __init__(self, enc_key_shares):
        self.enc_key_shares = []

    @property
    def size(self):
        return self.base_size + (32 + 20 + 32) * len(self.enc_key_shares)


# class DecKeySharesMessage(Message):


class Keyper(Peer):

    instance_counter = count()
    accepted_messages = [
        SecretSharesMessage,
        SecretShareWitnessesMessage,
        NoncesMessage,
        EncKeySharesMessage
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
        self.keypers_with_secret_shares = set()
        self.keypers_with_secret_share_witnesses = set()
        self.keypers_with_nonces = set()
        self.keypers_with_enc_key_share = set()
        # self.keypers_with_dec_key_share = set()

    def register_secret_share(self, sender):
        if sender != self.my_id:
            self.keypers_with_secret_shares.add(sender)

    def register_secret_share_witness(self, sender):
        if sender != self.my_id:
            self.keypers_with_secret_share_witnesses.add(sender)

    def register_nonce(self, sender):
        if sender != self.my_id:
            self.keypers_with_nonces.add(sender)

    def register_enc_key_share(self, sender):
        if sender != self.my_id:
            self.keypers_with_enc_key_share.add(sender)

    def key_distribution_finished(self):
        received_secret_shares = len(self.keypers_with_secret_shares)
        received_witnesses = len(self.keypers_with_secret_share_witnesses)
        return received_secret_shares == received_witnesses == self.n - 1

    def nonce_collection_finished(self):
        return self.key_distribution_finished() and len(self.keypers_with_nonces) == self.n - 1


class ThresholdEncryptionService(Service):

    def __init__(self, env, peer, n, k):
        super().__init__(env, peer)
        self.n = n
        self.k = k
        self.protocols = {}
        self.current_protocol = 0
        self.peer_to_known_secret_shares = {}
        self.peer_to_known_secret_share_witnesses = {}
        self.peer_to_known_nonces = {}
        self.peer_to_known_enc_key_shares = {}

    def handle_message(self, message, sender):
        if not isinstance(message, (
            SecretSharesMessage,
            SecretShareWitnessesMessage,
            NoncesMessage,
            EncKeySharesMessage
        )):
            return

        if isinstance(message, SecretSharesMessage):
            for block, sender, receiver in message.secret_shares:
                if block not in self.protocols:
                    self.kickoff_protocol(block)
                if receiver == self.peer.instance_number:
                    protocol = self.protocols[block]
                    protocol.register_secret_share(sender)
                    if protocol.key_distribution_finished():
                        self.send_nonce(block)

        if isinstance(message, SecretShareWitnessesMessage):
            for block, sender in message.secret_share_witnesses:
                if block not in self.protocols:
                    self.kickoff_protocol(block)
                protocol = self.protocols[block]
                protocol.register_secret_share_witness(sender)
                if protocol.key_distribution_finished():
                    self.send_nonce(block)

        if isinstance(message, NoncesMessage):
            for block, sender in message.nonces:
                if block not in self.protocols:
                    self.kickoff_protocol(block) 
                protocol = self.protocols[block]
                protocol.register_nonce(sender)
                if protocol.nonce_collection_finished():
                    self.send_enc_key_share(block)
                    if block == self.current_protocol:
                        self.current_protocol += 1
                        if self.current_protocol not in self.protocols:
                            self.kickoff_protocol(self.current_protocol)

    def kickoff_protocol(self, block):
        """Initialize protocol and transmit your secret shares and witnesses."""
        assert block not in self.protocols
        self.logger.info('kicking of protocol', block=block, time=self.env.now)
        protocol = ThresholdEncryptionProtocol(self.n, self.k, self.peer.instance_number)
        self.protocols[block] = protocol
        my_witness = SecretShareWitness(block, self.peer.instance_number)
        self.peer.threshold_distribution_service.witnesses.add(my_witness)
        my_id = self.peer.instance_number
        for i in chain(range(0, my_id), range(my_id + 1, self.n)):
            secret_share = SecretShare(block, self.peer.instance_number, i)
            self.peer.threshold_distribution_service.secret_shares.add(secret_share)

    def send_nonce(self, block):
        my_nonce = Nonce(block, self.peer.instance_number)
        if my_nonce not in self.peer.threshold_distribution_service.nonces:
            self.logger.info('publishing nonce', block=block, time=self.env.now)
            self.peer.threshold_distribution_service.nonces.add(my_nonce)
    
    def send_enc_key_share(self, block):
        my_enc_share = EncKeyShare(block, self.peer.instance_number)
        # if my_enc_share not in self.peer.threshold_distribution_service.enc_key_shares:
        #     self.logger.info('publishing encryption key share', block=block, time=self.env.now)
        #     self.peer.threshold_distribution_service.enc_key_shares.add(my_enc_share)

    def start(self):
        yield self.env.timeout(random.random() * 1)
        assert self.current_protocol == 0
        if self.current_protocol not in self.protocols:
            self.kickoff_protocol(self.current_protocol)


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
        if isinstance(message, SecretSharesMessage):
            self.secret_shares |= message.secret_shares
            self.peer_to_known_secret_shares[sender] |= message.secret_shares
        if isinstance(message, SecretShareWitnessesMessage):
            self.witnesses |= message.secret_share_witnesses
            self.peer_to_known_witnesses[sender] |= message.secret_share_witnesses
        if isinstance(message, NoncesMessage):
            self.nonces |= message.nonces
            self.peer_to_known_nonces[sender] |= message.nonces

    def start(self):
        # make sure not everyone starts at same time
        yield self.env.timeout(random.random() * self.resend_interval)
        while True:
            for other in self.peer.peers:
                messages = []
                new_secret_shares = self.secret_shares - self.peer_to_known_secret_shares[other]
                new_witnesses = self.witnesses - self.peer_to_known_witnesses[other]
                new_nonces = self.nonces - self.peer_to_known_nonces[other]
                if new_secret_shares:
                    # TODO only broadcast if not directly connected
                    messages.append(SecretSharesMessage(new_secret_shares))
                if new_witnesses:
                    messages.append(SecretShareWitnessesMessage(new_witnesses))
                if new_nonces:
                    messages.append(NoncesMessage(new_nonces))
                for message in messages:
                    self.env.process(self.peer.send(message, other))
            yield self.env.timeout(self.resend_interval)
