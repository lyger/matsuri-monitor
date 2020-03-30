from dataclasses import asdict, dataclass

import tornado.web


@dataclass
class Message:
    author: str
    text: str
    timestamp: float
    relative_timestamp: float

    def json(self) -> dict:
        """Return a JSON representation of this message"""
        return asdict(self)
