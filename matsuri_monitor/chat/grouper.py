from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List

import jsonschema
import tornado.options

from matsuri_monitor.chat.message import Message

tornado.options.define('grouper-file', default=Path('groupers.json'), type=Path, help='Path to grouper definitions json file')

GROUPER_SCHEMA = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'type': {'type': 'string', 'enum': ['username', 'regex']},
            'value': {'type': 'string'},
            'interval': {'type': 'number'},
            'min_len': {'type': 'number'},
            'notify': {'type': 'boolean'},
            'unique_author': {'type': 'boolean'},
            'skip_channels': {
                'type': 'array',
                'items': {'type': 'string'},
            },
        },
        'required': ['type', 'value', 'interval'],
    }
}


def _regex_condition(value: str) -> Callable[[Message], bool]:
    """Creates a condition that is true when message text matches the given regex"""
    exp = re.compile(value, flags=re.IGNORECASE)

    def condition(message: Message):
        return exp.search(message.text) is not None

    return condition


def _username_condition(value: str) -> Callable[[Message], bool]:
    """Creates a condition that is true when the message is by the given author"""
    def condition(message: Message):
        return message.author == value

    return condition


@dataclass
class Grouper:
    """Defines a grouper used to group chat messages for a report"""
    condition: Callable
    description: str
    interval: float
    min_len: int
    notify: bool
    unique_author: bool
    skip_channels: List[str]

    @classmethod
    def load(cls) -> List[Grouper]:
        """Load and return groupers defined in the passed JSON file"""
        grouper_defs = json.load(tornado.options.options.grouper_file.open())

        jsonschema.validate(grouper_defs, GROUPER_SCHEMA)

        groupers = []

        for gdef in grouper_defs:
            condition_type = gdef['type']
            condition_value = gdef['value']
            condition = globals()[f'_{condition_type}_condition'](condition_value)
            if condition_type == 'regex':
                description = f'Comment matches "{condition_value}"'
            elif condition_type == 'username':
                description = f'Comment from user "{condition_value}"'
            groupers.append(cls(
                condition=condition,
                description=description,
                interval=gdef['interval'],
                min_len=gdef.get('min_len', 1),
                notify=gdef.get('notify', False),
                unique_author=gdef.get('unique_author', False),
                skip_channels=gdef.get('skip_channels', []),
            ))

        return groupers
