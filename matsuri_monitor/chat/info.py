from dataclasses import dataclass

VIDEO_URL_TEMPLATE = "https://www.youtube.com/watch?v={video_id}"
CHANNEL_URL_TEMPLATE = "https://www.youtube.com/channel/{channel_id}"


@dataclass
class ChannelInfo:
    """Holds information about a YouTube channel"""

    id: str
    name: str
    thumbnail_url: str

    @property
    def url(self):
        """URL of the channel, constructed with the channel ID"""
        return CHANNEL_URL_TEMPLATE.format(channel_id=self.id)


@dataclass
class VideoInfo:
    """Holds information about a YouTube live stream (live or archive)"""

    id: str
    title: str
    channel: ChannelInfo
    start_timestamp: float = None

    @property
    def url(self):
        """URL of the video, constructed with the video ID"""
        return VIDEO_URL_TEMPLATE.format(video_id=self.id)
