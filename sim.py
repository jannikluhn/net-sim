import logging
import sys

import simpy
import structlog

import config
from networks import full_network
from main import ItemDistributorService


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
                # 'node': lambda p: isinstance(p, Keyper)
                # 'service': lambda s: s == ItemDistributorService
            }),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True)
        ],
        logger_factory=structlog.stdlib.LoggerFactory()
    )

    env = simpy.Environment()
    users, collator, keypers, validators = full_network(env)
    for peer in users + [collator] + keypers + validators:
        peer.start()
    # class M:
    #     size = 10
    # from main import *
    # peer1 = Peer(env, 10, 10)
    # peer2 = Peer(env, 10, 4)
    # peer3 = Peer(env, 10, 5)
    # env.process(peer1.send(M(), peer2))
    # env.process(peer1.send(M(), peer3))
    env.run(50)
