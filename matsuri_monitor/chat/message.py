from dataclasses import asdict, dataclass

import tornado.web


@dataclass
class Message:
    author: str
    text: str
    timestamp: float
    relative_timestamp: float

    @property
    def _type(self):
        return "message"

    def json(self) -> dict:
        """Return a JSON representation of this message"""
        d = asdict(self)
        d["type"] = self._type

        return d


@dataclass
class SuperChat(Message):
    amount: str

    @property
    def _type(self):
        return "superchat"
