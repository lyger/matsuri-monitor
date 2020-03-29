from typing import Callable, List

from matsuri_monitor.chat.grouper import Grouper
from matsuri_monitor.chat.message import Message


class GroupList:

    def __init__(self, grouper: Grouper):
        self.groups = []
        self.grouper = grouper
        self.description = grouper.description
        self.notify = grouper.notify
        self.last_timestamp = -float('inf')

    def update(self, messages: List[Message]):
        self.groups = []
        for message in messages:
            if self.grouper.condition(message):
                message_interval = message.timestamp - self.last_timestamp
                if message_interval <= self.grouper.interval:
                    self.add_to_last_group(message)
                else:
                    self.add_to_new_group(message)
        
        self.prune()
    
    def add_to_new_group(self, message: Message):
        self.groups.append([message])
        self.last_timestamp = message.timestamp
    
    def add_to_last_group(self, message: Message):
        if len(self.groups) == 0:
            return self.add_to_new_group(message)
        self.groups[-1].append(message)
        self.last_timestamp = message.timestamp

    def prune(self):
        self.groups = list(filter(lambda g: len(g) >= self.grouper.min_len, self.groups))

    def __len__(self):
        return len(self.groups)
