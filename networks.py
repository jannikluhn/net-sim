import random
import config
from user import User
from collator import Collator
from validator import Validator
from keyper import Keyper


def full_network(env):
    tx_interval = 1 / (config.TX_RATE / config.N_USERS)
    users = [
        User(env, config.USER_UPLINK, config.USER_DOWNLINK, tx_interval, config.KEYPER_THRESHOLD)
        for _ in range(config.N_USERS)
    ]
    collator = Collator(
        env,
        config.COLLATOR_UPLINK,
        config.COLLATOR_DOWNLINK,
        config.COLLATION_INTERVAL
    )
    keypers = [Keyper(env, config.KEYPER_UPLINK, config.KEYPER_DOWNLINK, config.KEYPER_THRESHOLD)
               for _ in range(config.N_KEYPERS)]
    validators = [
        Validator(env, config.VALIDATOR_UPLINK, config.VALIDATOR_DOWNLINK, config.KEYPER_THRESHOLD)
        for _ in range(config.N_KEYPERS)
    ]
    network = users + [collator] + keypers + validators

    # for now just randomly connect everyone with everyone
    for peer in network:
        while len(peer.peers) < config.N_USER_CONNECTIONS:
            other = random.choice(network)
            peer.connect(other)

    return users, collator, keypers, validators
