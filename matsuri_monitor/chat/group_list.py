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
        self.last_message_index = 0

    def update(self, messages: List[Message]):
        """Compute new groups from the given list of Messages"""
        new_group_indices = []
        for i in range(self.last_message_index, len(messages)):
            message = messages[i]
            if self.grouper.condition(message):
                message_interval = message.timestamp - self.last_timestamp
                if message_interval <= self.grouper.interval:
                    self.add_to_last_group(message)
                else:
                    # Delete last group if it's too short
                    if self.groups and len(self.groups[-1]) < self.grouper.min_len:
                        self.groups = self.groups[:-1]
                    new_group_indices.append(i)
                    self.add_to_new_group(message)

        # Delete last group if it's too short
        if self.groups and len(self.groups[-1]) < self.grouper.min_len:
            self.groups = self.groups[:-1]

        # Set to the start of the most recent (partial) group
        if new_group_indices:
            self.last_message_index = new_group_indices[-1]
        # If the group interval has already been exceeded, set to end of messages
        if (
            messages
            and (messages[-1].timestamp - messages[self.last_message_index].timestamp)
            > self.grouper.interval
        ):
            self.last_message_index = len(messages) - 1

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
