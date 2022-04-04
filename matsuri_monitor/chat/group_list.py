from typing import Callable, List

from matsuri_monitor.chat.grouper import Grouper
from matsuri_monitor.chat.message import Message


class GroupList:
    def __init__(self, grouper: Grouper):
        """init

        Parameters
        ----------
        grouper
            Grouper used to define this group list
        """
        self.groups = []
        self.grouper = grouper
        self.description = grouper.description
        self.notify = grouper.notify
        self.last_timestamp = -float("inf")

    def update(self, messages: List[Message]):
        """Recompute groups from the given list of Messages"""
        self.groups = []
        for message in messages:
            if self.grouper.condition(message):
                message_interval = message.timestamp - self.last_timestamp
                if message_interval <= self.grouper.interval:
                    self.add_to_last_group(message)
                else:
                    self.add_to_new_group(message)

        self.groups = list(
            filter(lambda g: len(g) >= self.grouper.min_len, self.groups)
        )

    def add_to_new_group(self, message: Message):
        """Add message to a new group"""
        self.groups.append([message])
        self.last_timestamp = message.timestamp

    def add_to_last_group(self, message: Message):
        """Add message to current last group"""
        if len(self.groups) == 0:
            return self.add_to_new_group(message)
        if self.grouper.unique_author and any(
            message.author == other.author for other in self.groups[-1]
        ):
            return
        self.groups[-1].append(message)
        self.last_timestamp = message.timestamp

    def __len__(self):
        """Number of groups in this list"""
        return len(self.groups)
