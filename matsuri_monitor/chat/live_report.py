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
        self.info = info
        self.group_lock = mp.Lock()
        self.group_lists: List[GroupList] = []
        self.messages: List[Message] = []
        self.__finalized = False

    def set_groupers(self, groupers: List[Grouper]):
        with self.group_lock:
            self.group_lists = list(map(GroupList, groupers))
        self.add_messages([])

    def add_messages(self, messages: List[Message]):
        if self.__finalized:
            raise RuntimeError('Cannot modify a finalized LiveReport')

        self.messages.extend(messages)

        # Sort and deduplicate
        self.messages.sort(key=lambda msg: msg.timestamp)
        self.messages = [dup[0] for dup in groupby(self.messages)]

        with self.group_lock:
            for group_list in self.group_lists:
                group_list.update(self.messages)
    
    def finalize(self):
        # Drop message buffer to save memory; we only keep the groups
        self.messages.clear()
        # Permanently cache the JSON representation
        self.json = cached(LRUCache(1))(self.json)
        self.__finalized = True

    def json(self):
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
        return sum(map(len, self.group_lists))
