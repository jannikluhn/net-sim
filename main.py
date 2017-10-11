from itertools import count
import math
import random

import structlog


class Message(object):

    base_size = 20

    @property
    def size(self):
        return self.base_size


class Peer(object):

    instance_counter = count()
    accepted_messages = []

    def __init__(self, env, uplink, downlink):
        self.env = env
        self.services = []
        self.instance_number = next(self.instance_counter)

        self.logger = structlog.get_logger(self.__class__.__name__ + str(self.instance_number))
        self.logger = self.logger.bind(node=self)

        self.uplink = uplink
        self.downlink = downlink

        self.uplink_allocations = []  # [(bandwidth, used until), ...]
        self.downlink_allocations = []  # [(bandwidth, used until), ...]

        self.peers = []

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.instance_number)

    def start(self):
        for service in self.services:
            self.env.process(service.start())

    def send(self, message, receiver):
        # don't send message if receiver isn't interested
        if not any(isinstance(message, MessageType) for MessageType in receiver.accepted_messages):
            return
        transmitted = 0
        not_transmitted = message.size
        time = self.env.now

        uplink_allocations = iter(self.uplink_allocations)
        downlink_allocations = iter(receiver.downlink_allocations)
        new_uplink_allocations = []
        new_downlink_allocations = []

        # skip historic allocations
        uplink_used_until = -math.inf
        while uplink_used_until < self.env.now:
            uplink_used, uplink_used_until = next(uplink_allocations, (0, math.inf))
        downlink_used_until = -math.inf
        while downlink_used_until < self.env.now:
            downlink_used, downlink_used_until = next(downlink_allocations, (0, math.inf))

        while not math.isclose(transmitted, message.size):
            # check how much bandwidth is available
            if time >= uplink_used_until:
                uplink_used, uplink_used_until = next(uplink_allocations, (0, math.inf))
            if time >= downlink_used_until:
                downlink_used, downlink_used_until = next(uplink_allocations, (0, math.inf))

            # allocate bandwidth
            bandwidth = min(self.uplink - uplink_used, receiver.downlink - downlink_used)
            if bandwidth != 0:
                expected_transmission_time = float(not_transmitted) / bandwidth
            else:
                expected_transmission_time = math.inf
            bandwidth_used_until = min([
                uplink_used_until,
                downlink_used_until,
                time + expected_transmission_time
            ])
            new_uplink_allocations.append((uplink_used + bandwidth, bandwidth_used_until))
            new_downlink_allocations.append((downlink_used + bandwidth, bandwidth_used_until))

            # calculate transmission time
            transmitted += bandwidth * (bandwidth_used_until - time)
            not_transmitted = message.size - transmitted
            time = bandwidth_used_until

        # replay other allocations
        new_uplink_allocations.append((uplink_used, uplink_used_until))
        new_downlink_allocations.append((downlink_used, downlink_used_until))
        for uplink_used, uplink_used_until in uplink_allocations:
            new_uplink_allocations.append((uplink_used, uplink_used_until))
        for downlink_used, downlink_used_until in downlink_allocations:
            new_downlink_allocations.append((downlink_used, downlink_used_until))
        self.uplink_allocations = new_uplink_allocations
        receiver.downlink_allocations = new_downlink_allocations

        # sleep and receive
        yield self.env.timeout(time - self.env.now)
        receiver.receive(message, self)

    def receive(self, message, sender):
        for service in self.services:
            service.handle_message(message, sender)

    def connect(self, peer):
        if peer is self:
            return
        if peer not in self.peers:
            self.logger.info('connecting', to=peer, time=self.env.now)
            self.peers.append(peer)
            peer.connect(self)


class Service(object):

    def __init__(self, env, peer):
        self.env = env
        self.peer = peer
        self.logger = self.peer.logger.bind(service=self.__class__.__name__)

    def handle_message(self, message, sender):
        pass

    def start(self):
        pass
