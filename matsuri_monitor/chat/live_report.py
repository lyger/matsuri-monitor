import gzip
import json
import multiprocessing as mp
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from itertools import groupby
from pathlib import Path
from typing import Callable, List

from cachetools import LRUCache, cached
import tornado.options

from matsuri_monitor.chat.group_list import GroupList
from matsuri_monitor.chat.grouper import Grouper
from matsuri_monitor.chat.info import VideoInfo
from matsuri_monitor.chat.message import Message

tornado.options.define('archives-dir', default=Path('archives'), type=Path, help='Path to save archive JSONs')
tornado.options.define('dump-chat', default=False, type=bool, help='Also dump all stream comments to archive dir')


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

    def set_groupers(self, groupers: List[Grouper]):
        """Set the groupers used to generate this report"""
        with self.message_lock:
            messages = self.messages

        include = lambda g: self.info.channel.id not in g.skip_channels
        with self.group_lock:
            self.group_lists = list(map(GroupList, filter(include, groupers)))
            for group_list in self.group_lists:
                group_list.update(messages)

    def add_messages(self, new_messages: List[Message]):
        """Add new messages and recompute groups from them"""
        with self.message_lock:
            self.messages.extend(new_messages)

            # Sort and deduplicate
            self.messages.sort(key=lambda msg: msg.timestamp)
            self.messages = [dup[0] for dup in groupby(self.messages)]
            messages = self.messages

        with self.group_lock:
            for group_list in self.group_lists:
                group_list.update(messages)

    def save(self):
        """Save report to archives directory and finalize"""
        report_datetime = datetime.fromtimestamp(self.info.start_timestamp).isoformat(timespec='seconds')
        report_basename = f'{report_datetime}_{self.info.id}'.replace(':', '')
        report_path = tornado.options.options.archives_dir / f'{report_basename}.json.gz'

        if tornado.options.options.dump_chat:
            messages_json = [msg.json() for msg in self.messages]
            messages_path = tornado.options.options.archives_dir / f'{report_basename}_chat.json.gz'

            with gzip.open(messages_path, 'wt') as dump_file:
                json.dump(messages_json, dump_file)

        if len(self) == 0:
            return

        with gzip.open(report_path, 'wt') as report_file:
            json.dump(self.json(), report_file)

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
                    for group_list in filter(lambda gl: len(gl) > 0, self.group_lists)
                ]
            }

        return ret

    def __len__(self):
        """The total number of groups in this report, across all lists"""
        with self.group_lock:
            return sum(map(len, self.group_lists))
