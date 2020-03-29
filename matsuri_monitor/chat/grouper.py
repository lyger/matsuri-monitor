from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List

import jsonschema
import tornado.options

from matsuri_monitor.chat.message import Message

tornado.options.define('grouper-file', type=Path, default=Path('groupers.json'), help='Path to grouper definitions json file')

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
        }
    }
}


def _regex_condition(value: str) -> Callable[[Message], bool]:
    exp = re.compile(value)

    def condition(message: Message):
        return exp.search(message.text) is not None

    return condition


def _username_condition(value: str) -> Callable[[Message], bool]:
    def condition(message: Message):
        return message.author == value

    return condition


@dataclass
class Grouper:
    condition: Callable
    description: str
    interval: float
    min_len: int
    notify: bool

    @classmethod
    def load(cls) -> List[Grouper]:
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
                min_len=gdef['min_len'],
                notify=gdef['notify'],
            ))

        return groupers
