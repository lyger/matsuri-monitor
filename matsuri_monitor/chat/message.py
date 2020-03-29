from dataclasses import dataclass

import tornado.web


@dataclass
class Message:
    author: str
    text: str
    timestamp: float
    relative_timestamp: float
