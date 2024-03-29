import gzip
import json
import multiprocessing as mp
from datetime import datetime
from itertools import groupby
from pathlib import Path
from typing import List

import tornado.options

from matsuri_monitor.chat.group_list import GroupList
from matsuri_monitor.chat.grouper import Grouper
from matsuri_monitor.chat.info import VideoInfo
from matsuri_monitor.chat.message import Message

SAVE_ORGS = ["Hololive"]

tornado.options.define(
    "archives-dir",
    default=Path("archives"),
    type=Path,
    help="Path to save archive JSONs",
)
tornado.options.define(
    "dump-chat",
    default=False,
    type=bool,
    help="Also dump all stream comments to archive dir",
)


def combine_reports(report1: dict, report2: dict):
    new_report = dict(report1)
    gls1 = report1["group_lists"]
    gls2 = report2["group_lists"]
    desc2i1 = {gl["description"]: i for i, gl in enumerate(gls1)}
    desc2i2 = {gl["description"]: i for i, gl in enumerate(gls2)}
    all_descs = list(set(list(desc2i1) + list(desc2i2)))
    new_gls = []
    for desc in all_descs:
        # Only in report 2
        if desc not in desc2i1:
            new_gls.append(dict(gls2[desc2i2[desc]]))
        # Only in report 1
        elif desc not in desc2i2:
            new_gls.append(dict(gls1[desc2i1[desc]]))
        # In both
        else:
            gl1 = gls1[desc2i1[desc]]
            gl2 = gls2[desc2i2[desc]]
            new_groups = gl1["groups"] + gl2["groups"]
            new_groups.sort(key=lambda g: g["timestamp"])
            new_gl = dict(gl1)
            new_gl["groups"] = new_groups
            new_gls.append(new_gl)
    new_report["group_lists"] = new_gls
    return new_report


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
        report_datetime = datetime.fromtimestamp(self.info.start_timestamp).isoformat(
            timespec="seconds"
        )
        report_basename = f"{report_datetime}_{self.info.id}".replace(":", "")
        report_path = (
            tornado.options.options.archives_dir / f"{report_basename}.json.gz"
        )

        if tornado.options.options.dump_chat and self.info.channel.org in SAVE_ORGS:
            messages_json = [msg.json() for msg in self.messages]
            messages_path = (
                tornado.options.options.archives_dir / f"{report_basename}_chat.json.gz"
            )

            if messages_path.exists():
                with gzip.open(messages_path, "rt") as existing_file:
                    existing_messages = json.load(existing_file)
                messages_json = existing_messages + messages_json

            with gzip.open(messages_path, "wt") as dump_file:
                json.dump(messages_json, dump_file)

        if len(self) == 0:
            return

        report_json = self.json()

        if report_path.exists():
            with gzip.oepn(report_path, "rt") as existing_file:
                existing_report = json.load(existing_file)
            report_json = combine_reports(existing_report, report_json)

        with gzip.open(report_path, "wt") as report_file:
            json.dump(report_json, report_file)

    def json(self) -> dict:
        """Return a JSON representation of this report"""
        with self.group_lock:
            ret = {
                "id": self.info.id,
                "url": self.info.url,
                "title": self.info.title,
                "channel_url": self.info.channel.url,
                "channel_name": self.info.channel.name,
                "thumbnail_url": self.info.channel.thumbnail_url,
                "group_lists": [
                    {
                        "description": group_list.description,
                        "notify": group_list.notify,
                        "groups": [
                            [
                                {
                                    "author": message.author,
                                    "text": message.text,
                                    "timestamp": message.timestamp,
                                    "relative_timestamp": message.relative_timestamp,
                                }
                                for message in group
                            ]
                            for group in group_list.groups
                        ],
                    }
                    for group_list in filter(lambda gl: len(gl) > 0, self.group_lists)
                ],
            }

        return ret

    def __len__(self):
        """The total number of groups in this report, across all lists"""
        with self.group_lock:
            return sum(map(len, self.group_lists))
