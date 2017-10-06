import logging
import random
import sys

import simpy
import structlog

import config
from peers import User, Collator


def setup_network(env):
    tx_spawn_interval = 1. / (config.TX_RATE / config.N_USERS)
    users = [User(env, config.USER_UPLINK, config.USER_DOWNLINK, tx_spawn_interval)
             for _ in range(config.N_USERS)]
    for user in users:
        while len(user.peers) < config.N_USER_CONNECTIONS:
            other = random.choice(users)
            user.connect(other)
    
    collator = Collator(
        env,
        config.COLLATOR_UPLINK,
        config.COLLATOR_DOWNLINK,
        config.COLLATION_INTERVAL
    )
    for user in users[:config.N_COLLATOR_CONNECTIONS]:
        collator.connect(user)

    return users + [collator]


class LogFilter(object):

    def __init__(self, conditions=None):
        self.conditions = conditions or {}

    def __call__(self, logger, method_name, event_dict):
        for key, condition in self.conditions.items():
            if key in event_dict and not condition(event_dict[key]):
                raise structlog.DropEvent
        return event_dict


if __name__ == '__main__':
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO
    )
    structlog.configure(
        processors=[
            LogFilter({
                'node': lambda p: isinstance(p, Collator)
            }),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True)
        ],
        logger_factory=structlog.stdlib.LoggerFactory()
    )

    env = simpy.Environment()
    network = setup_network(env)
    for peer in network:
        peer.start()
    env.run(100)
