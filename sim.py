import logging
import random
import sys

import simpy
import structlog

import config
from peers import User


if __name__ == '__main__':
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO
    )
    structlog.configure(
        processors=[
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True)
        ],
        logger_factory=structlog.stdlib.LoggerFactory()
    )

    env = simpy.Environment()
    tx_spawn_interval = 1. / (config.TX_SPAWN_FREQUENCY / config.N_USERS)
    users = [User(env, config.USER_UPLINK, config.USER_DOWNLINK, tx_spawn_interval)
             for _ in range(config.N_USERS)]
    for user in users:
        while len(user.peers) < config.N_USER_CONNECTIONS:
            other = random.choice(users)
            user.connect(other)
    for user in users:
        user.start()
    env.run(100)
