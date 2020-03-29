from dataclasses import dataclass

VIDEO_URL_TEMPLATE = 'https://www.youtube.com/watch?v={video_id}'
CHANNEL_URL_TEMPLATE = 'https://www.youtube.com/channel/{channel_id}'


@dataclass
class ChannelInfo:
    id: str
    name: str
    thumbnail_url: str
    
    @property
    def url(self):
        return CHANNEL_URL_TEMPLATE.format(channel_id=self.id)


@dataclass
class VideoInfo:
    id: str
    title: str
    channel: ChannelInfo
    start_timestamp: float = None

    @property
    def url(self):
        return VIDEO_URL_TEMPLATE.format(video_id=self.id)
