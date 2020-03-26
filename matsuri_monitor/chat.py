from dataclasses import dataclass

@dataclass
class Message:
    author: str
    text: str
    timestamp: float
