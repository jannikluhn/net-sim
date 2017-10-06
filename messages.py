class Message(object):

    base_size = 20

    @property
    def size(self):
        return self.base_size


class NewTransactionsMessage(Message):

    def __init__(self, transactions):
        self.transactions = transactions

    @property
    def size(self):
        return sum(len(tx) for tx in self.transactions)

    def __repr__(self):
        return '<NewTxsMessage n={}>'.format(len(self.transactions))


class CollationMessage(Message):

    def __init__(self, transactions):
        self.transactions = transactions

    @property
    def size(self):
        return sum(len(tx) for tx in self.transactions)

    def __repr__(self):
        return '<CollationMessage n={}>'.format(len(self.transactions))
