import multiprocessing as mp
from collections import OrderedDict
from dataclasses import dataclass
from itertools import groupby
from typing import Callable, List

from cachetools import LRUCache, cached

from matsuri_monitor.chat.group_list import GroupList
from matsuri_monitor.chat.grouper import Grouper
from matsuri_monitor.chat.info import VideoInfo
from matsuri_monitor.chat.message import Message


class LiveReport:

    def __init__(self, info: VideoInfo):
        """init

        Parameters
        ----------
        info
            VideoInfo describing the video to generate a report for
        """
        self.info = info
        self.group_lock = mp.Lock()
        self.group_lists: List[GroupList] = []
        self.message_lock = mp.Lock()
        self.messages: List[Message] = []
        self.__finalized = False

    def set_groupers(self, groupers: List[Grouper]):
        """Set the groupers used to generate this report"""
        if self.__finalized:
            raise RuntimeError('Cannot modify a finalized LiveReport')

        include = lambda g: self.info.channel.id not in g.skip_channels
        with self.group_lock:
            self.group_lists = list(map(GroupList, filter(include, groupers)))

        self.add_messages([])

    def add_messages(self, messages: List[Message]):
        """Add new messages and recompute groups from them"""
        if self.__finalized:
            raise RuntimeError('Cannot modify a finalized LiveReport')

        with self.message_lock:
            self.messages.extend(messages)

            # Sort and deduplicate
            self.messages.sort(key=lambda msg: msg.timestamp)
            self.messages = [dup[0] for dup in groupby(self.messages)]

        with self.group_lock:
            for group_list in self.group_lists:
                group_list.update(self.messages)

    def finalize(self):
        """Clean up and freeze state of report once live ends"""
        # Drop message buffer to save memory; we only keep the groups
        with self.message_lock:
            self.messages.clear()

        # Drop group lists with no groups in them
        with self.group_lock:
            self.group_lists = list(filter(lambda gl: len(gl) > 0, self.group_lists))

        # Turn off notifications
        for group_list in self.group_lists:
            group_list.notify = False

        # Permanently cache the JSON representation
        self.json = cached(LRUCache(1))(self.json)
        self.__finalized = True

    def json(self) -> dict:
        """Return a JSON representation of this report"""
        with self.group_lock:
            ret = {
                'id': self.info.id,
                'url': self.info.url,
                'title': self.info.title,
                'channel_url': self.info.channel.url,
                'channel_name': self.info.channel.name,
                'thumbnail_url': self.info.channel.thumbnail_url,
                'group_lists': [
                    {
                        'description': group_list.description,
                        'notify': group_list.notify,
                        'groups': [
                            [
                                {
                                    'author': message.author,
                                    'text': message.text,
                                    'timestamp': message.timestamp,
                                    'relative_timestamp': message.relative_timestamp,
                                }
                                for message in group
                            ]
                            for group in group_list.groups
                        ]
                    }
                    for group_list in self.group_lists
                ]
            }

        return ret

    def __len__(self):
        """The total number of groups in this report, across all lists"""
        with self.group_lock:
            return sum(map(len, self.group_lists))
