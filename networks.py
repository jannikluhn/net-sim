import random
from keyper import Keyper


def keyper_network(env, uplink, downlink, n, k, n_connections):
    keypers = []
    for _ in range(n):
        keyper = Keyper(env, uplink, downlink, k)
        keypers.append(keyper)
    for keyper in keypers:
        while len(keyper.peers) < n_connections:
            other = random.choice(keypers)
            keyper.connect(other)
    return keypers
