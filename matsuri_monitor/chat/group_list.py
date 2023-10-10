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
        self._groups = []
        self.grouper = grouper
        self.description = grouper.description
        self.notify = grouper.notify
        self.last_timestamp = -float("inf")
        self.last_message_index = 0

    def update(self, messages: List[Message]):
        """Compute new groups from the given list of Messages"""
        for message in messages[self.last_message_index :]:
            if self.grouper.condition(message):
                message_interval = message.timestamp - self.last_timestamp
                if message_interval <= self.grouper.interval:
                    self.add_to_last_group(message)
                else:
                    self.add_to_new_group(message)

        self.last_message_index = len(messages)

    def add_to_new_group(self, message: Message):
        """Add message to a new group"""
        self._groups.append([message])
        self.last_timestamp = message.timestamp

    def add_to_last_group(self, message: Message):
        """Add message to current last group"""
        if len(self._groups) == 0:
            return self.add_to_new_group(message)
        if self.grouper.unique_author and any(
            message.author == other.author for other in self._groups[-1]
        ):
            return
        self._groups[-1].append(message)
        self.last_timestamp = message.timestamp

    @property
    def groups(self) -> List[List[Message]]:
        """Return filtered groups."""
        return list(filter(lambda g: len(g) >= self.grouper.min_len, self._groups))

    def __len__(self):
        """Number of groups in this list"""
        return len(self.groups)
