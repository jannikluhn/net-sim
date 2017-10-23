from itertools import count
from main import Peer, Service, Collation, Vote


class Validator(Peer):

    instance_counter = count()

    def __init__(self, env, uplink, downlink, keyper_threshold):
        super().__init__(env, uplink, downlink)
        self.validator_service = ValidatorService(self.env, self, keyper_threshold)
        self.services.append(self.validator_service)


class ValidatorService(Service):

    def __init__(self, env, peer, keyper_threshold):
        super().__init__(env, peer)
        self.current_block = 0
        self.keyper_threshold = keyper_threshold

    def start(self):
        while True:
            logger = self.logger.bind(block=self.current_block)
            yield self.env.process(self.wait_for_collation())
            logger.info('received collation', time=self.env.now)
            yield self.env.process(self.wait_for_dec_key())
            logger.info('received decryption key', time=self.env.now)
            self.vote()
            logger.info('voted', time=self.env.now)
            self.current_block += 1

    def wait_for_collation(self):
        collations = set()
        while not collations:
            collations = yield self.env.process(self.peer.distributor.get_items(
                Collation.type_id,
                self.current_block
            ))
            assert len(collations) <= 1

    def vote(self):
        self.logger.info('voting', block=self.current_block, time=self.env.now)
        vote = Vote(self.current_block, self.peer.instance_number)
        self.peer.distributor.distribute(vote)

    def wait_for_dec_key(self):
        dec_keys = set()
        while len(dec_keys) < self.keyper_threshold:
            dec_keys |= yield self.env.process(self.peer.distributor.get_items(
                Collation.type_id,
                self.current_block,
                exclude=dec_keys
            ))
