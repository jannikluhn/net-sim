from collections import defaultdict, namedtuple
from collections.abc import Container
from itertools import count
import math
import random

from simpy.events import AllOf
import structlog


BLOCK_NUMBER_SIZE = 4
ITEM_HASH_SIZE = 32
ADDRESS_SIZE = 20
SIGNATURE_SIZE = 132


class Message(object):

    base_size = 20

    @property
    def size(self):
        return self.base_size


class AnnounceItems(Message):

    def __init__(self, items):
        self.items = items

    @property
    def size(self):
        return super().size + len(self.items) * (BLOCK_NUMBER_SIZE + ITEM_HASH_SIZE)


class RequestItems(Message):

    def __init__(self, hashes):
        self.hashes = hashes

    @property
    def size(self):
        return super().size + len(self.hashes) * ITEM_HASH_SIZE


class SendItems(Message):

    def __init__(self, items):
        self.items = items

    @property
    def size(self):
        return super().size + sum(item.size for item in self.items)


class Item(object):

    item_type_counter = count()
    size = BLOCK_NUMBER_SIZE + ITEM_HASH_SIZE

    def __init__(self, block):
        self.block = block

    def __eq__(self, other):
        return hash(self) == hash(other)


class SignedItem(Item):
    """Item with a sender address and signature."""

    size = Item.size + SIGNATURE_SIZE

    def __init__(self, block, sender):
        super().__init__(block)
        self.sender = sender

    def __hash__(self):
        return hash((self.__class__.type_id, self.block, self.sender))

    def __repr__(self):
        return '<{} block={} sender={}>'.format(self.__class__.__name__, self.block, self.sender)


class AddressedItem(SignedItem):
    """Item with both a sender and a receiver address."""

    size = SignedItem.size + ADDRESS_SIZE

    def __init__(self, block, sender, receiver):
        super().__init__(block, sender)
        self.receiver = receiver

    def __hash__(self):
        return hash((self.__class__.type_id, self.block, self.sender, self.receiver))

    def __repr__(self):
        return '<{} block={} sender={} receiver={}>'.format(
            self.__class__.__name__,
            self.block,
            self.sender,
            self.receiver
        )


class Transaction(Item):

    size = Item.size + 100
    type_id = next(Item.item_type_counter)


class SecretShare(AddressedItem):

    size = AddressedItem.size + 2 * 32
    type_id = next(Item.item_type_counter)


class Witness(SignedItem):

    size = SignedItem.size + 32
    type_id = next(Item.item_type_counter)


class Nonce(SignedItem):

    size = SignedItem.size + 32
    type_id = next(Item.item_type_counter)


class EncKeyShare(SignedItem):

    size = SignedItem.size + 32
    type_id = next(Item.item_type_counter)


class DecKeyShare(SignedItem):

    size = SignedItem.size + 32
    type_id = next(Item.item_type_counter)


class Vote(SignedItem):

    size = SignedItem.size + 32
    type_id = next(Item.item_type_counter)


class Peer(object):
    """A node in the network."""

    instance_counter = count()

    def __init__(self, env, uplink, downlink):
        self.instance_number = next(self.instance_counter)
        self.env = env
        self.peers = []

        self.logger = structlog.get_logger(self.__class__.__name__ + str(self.instance_number))
        self.logger = self.logger.bind(node=self)

        self.uplink = uplink
        self.downlink = downlink

        self.uplink_allocations = []  # [(bandwidth, used until), ...]
        self.downlink_allocations = []  # [(bandwidth, used until), ...]

        self.transmission_events_by_peer = defaultdict(list)

        self.distributor = ItemDistributorService(self.env, self)
        self.services = [self.distributor]

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.instance_number)

    def start(self):
        """Start the node by starting all registered services."""
        for service in self.services:
            self.env.process(service.start())

    def send(self, message, receiver):
        """Send a message to a connected peer."""
        # don't send message if receiver isn't interested
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
        transmission_event = self.env.timeout(time - self.env.now)
        self.transmission_events_by_peer[receiver].append(transmission_event)
        yield transmission_event
        self.transmission_events_by_peer[receiver].remove(transmission_event)
        receiver.receive(message, self)

    def receive(self, message, sender):
        """Called when a message to this peer has been fully transmitted."""
        for service in self.services:
            service.handle_message(message, sender)

    def connect(self, peer):
        """Establish bidirectional connection to another peer."""
        if peer is self:
            return
        if peer not in self.peers:
            self.logger.info('connecting', to=peer, time=self.env.now)
            self.peers.append(peer)
            peer.connect(self)

    def is_sending_to(self, peer):
        """True iff this peer is sending a message to another peer at the moment."""
        return bool(self.transmission_events_by_peer[peer])

    def is_connection_busy(self, peer):
        """True iff a message is sent between this peer and another, no matter the direction."""
        return self.is_sending_to(peer) or peer.is_sending_to(self)


class Service(object):

    def __init__(self, env, peer):
        self.env = env
        self.peer = peer
        self.logger = self.peer.logger.bind(service=self.__class__.__name__)

    def handle_message(self, message, sender):
        pass

    def start(self):
        pass


class ItemDistributorService(Service):

    def __init__(self, env, peer):
        super().__init__(env, peer)
        self.known_items = set()  # items that at least one of our peers have
        self.fetched_items = set()  # items that we've already downloaded
        self.items_by_peer = defaultdict(set)
        self.next_fetch_event = self.env.event()

    def handle_message(self, message, sender):
        logger = self.logger.bind(time=self.env.now, **{'from': sender})
        if isinstance(message, AnnounceItems):
            # take note of new available items
            items = set(message.items)
            self.items_by_peer[sender] |= items
            n_new = len(items - self.known_items)
            logger.info('receiving announcement', total=len(items), new=n_new)
            self.known_items |= items
        if isinstance(message, SendItems):
            # take note of newly fetched items
            items = set(message.items)
            n_new = len(items - self.fetched_items)
            logger.info('receiving items', total=len(items), new=n_new)
            self.items_by_peer[sender] |= items
            self.known_items |= items
            self.fetched_items |= items
            if n_new > 0:
                self.next_fetch_event.succeed()
                self.next_fetch_event = self.env.event()
        if isinstance(message, RequestItems):
            # answer request as well as possible
            items = set()
            for hash_ in message.hashes:
                for item in self.fetched_items:
                    if hash(item) == hash_:
                        items.add(item)
            logger.info('receiving request', total=len(message.hashes), known=len(items))
            reply = SendItems(items)
            self.env.process(self.peer.send(reply, sender))

    def get_items(self, type_ids, block, exclude=None):
        exclude = exclude or set()
        items = set()
        fetched_once = False
        if not isinstance(type_ids, Container):
            type_ids = [type_ids]
        while not items:
            # check in items we've already fetched
            for item in self.fetched_items:
                if item.type_id not in type_ids:
                    continue
                if block is not None and item.block != block:
                    continue
                if item in exclude:
                    continue
                items.add(item)

            if fetched_once:
                break

            # nothing matched, wait until new ones are sent by peers
            if not items:
                yield self.next_fetch_event
            fetched_once = True
        return items

    def distribute(self, item):
        """Add an item to the local distribution set and start announcing it to the network."""
        self.known_items.add(item)
        self.fetched_items.add(item)

    def announce_request_loop(self, peer):
        while True:
            # announce
            items = self.items_by_peer[peer]
            new_items = self.fetched_items - items
            if new_items and not self.peer.is_connection_busy(peer):
                message = AnnounceItems(new_items)
                yield self.env.process(self.peer.send(message, peer))
                self.items_by_peer[peer] |= new_items
            # request
            items = self.items_by_peer[peer]
            new_items = items - self.fetched_items
            if new_items and not self.peer.is_connection_busy(peer):
                message = RequestItems([hash(item) for item in new_items])
                yield self.env.process(self.peer.send(message, peer))
            # sleep
            yield self.env.timeout(1)

    def start(self):
        processes = []
        for peer in self.peer.peers:
            process = self.env.process(self.announce_request_loop(peer))
            processes.append(process)
        yield self.env.all_of(processes)
