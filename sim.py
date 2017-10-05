import simpy


class Message(object):

    base_size = 20

    def __init__(self, payload):
        self.payload = payload

    @property
    def size(self):
        return self.base_size + len(self.payload)


class Service(object):
    
    def handle_message(self, message, sender, receiver):
        raise NotImplementedError()


class MessageLogger(Service):

    def handle_message(self, message, sender, receiver):
        print '{} received by {} from {} at {}'.format(message.payload, receiver.name, sender.name,
                                                       sender.env.now)



class Peer(object):

    def __init__(self, env, name, uplink, downlink):
        self.env = env
        self.name = name
        self.services = []

        self.uplink = simpy.Container(self.env, init=uplink)
        self.downlink = simpy.Container(self.env, init=downlink)
        self.send_processes = []
        self.receive_processes = []

    def send(self, receiver, message):
        def _send():
            transmitted = 0
            complete = False
            while not complete:
                # find available bandwidth (can be zero)
                bandwidth = min(self.uplink.level, receiver.downlink.level)
                # consume bandwidth
                if bandwidth > 0:
                    self.uplink.get(bandwidth)
                    receiver.downlink.get(bandwidth)
                # send message
                start = self.env.now
                try:
                    to_transmit = float(message.size) - transmitted
                    time = to_transmit / bandwidth if bandwidth != 0 else float('inf')
                    yield self.env.timeout(time)
                except simpy.Interrupt:
                    # interrupted because other process finished, redistributing bandwidth
                    pass
                else:
                    complete = True
                transmitted += bandwidth * (self.env.now - start)
                if bandwidth > 0:
                    self.uplink.put(bandwidth)
                    receiver.downlink.put(bandwidth)
            # transfer finished, interrupt all other transfers to redistribute bandwidth
            self.send_processes.remove(proc)
            receiver.receive_processes.remove(proc)
            for process in self.send_processes + receiver.receive_processes:
                process.interrupt()
            receiver.receive(message, self)
        proc = self.env.process(_send())
        self.send_processes.append(proc)
        receiver.receive_processes.append(proc)

    def receive(self, message, sender):
        for s in self.services:
            s.handle_message(message, sender, self)


env = simpy.Environment()
peers = [
    Peer(env, 'Alice', 10, 5),
    Peer(env, 'Bob', 5, 5),
    Peer(env, 'Charlie', 5, 5),
    Peer(env, 'Dave', 5, 5)
]
for peer in peers:
    peer.services.append(MessageLogger())
message = Message('Hello!')
peers[0].send(peers[1], message)
peers[0].send(peers[2], message)
peers[0].send(peers[3], message)
env.run(100)
