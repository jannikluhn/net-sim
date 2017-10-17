import logging
import sys

import simpy
import structlog

import config
from networks import keyper_network



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
                #'node': lambda p: isinstance(p, Keyper)
            }),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True)
        ],
        logger_factory=structlog.stdlib.LoggerFactory()
    )

    env = simpy.Environment()
    network = keyper_network(
        env,
        config.KEYPER_UPLINK,
        config.KEYPER_DOWNLINK,
        config.N_KEYPERS,
        config.KEYPER_THRESHOLD,
        config.N_KEYPER_CONNECTIONS
    )
    for peer in network:
        peer.start()
    env.run(50)
