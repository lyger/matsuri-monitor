import multiprocessing as mp

import pandas as pd
import requests

from matsuri_monitor import chat

CHANNEL_ENDPOINT = 'https://storage.googleapis.com/vthell-data/channels.json'
LIVE_ENDPOINT = 'https://storage.googleapis.com/vthell-data/live.json'
VIDEO_ENDPOINT = 'https://storage.googleapis.com/vthell-data/videos.json'


class Jetri:

    def __init__(self):
        channels = requests.get(CHANNEL_ENDPOINT).json()['channels']
        self._lock = mp.Lock()
        self.channels = pd.DataFrame.from_records(channels, index='id')
        self.lives = pd.DataFrame(index=pd.Series(name='id'), columns=['title', 'type', 'startTime', 'channel'])

    def update(self):
        lives = requests.get(LIVE_ENDPOINT).json()
        videos = requests.get(VIDEO_ENDPOINT).json()['videos']

        with self._lock:
            to_concat = []
            for chid, records in lives.items():
                channel_lives = pd.DataFrame.from_records(
                    records, index='id', columns=['id', 'title', 'type', 'startTime']
                )
                channel_lives['channel'] = chid
                to_concat.append(channel_lives)

            self.lives = pd.concat(to_concat)

    @property
    def currently_live(self):
        return self.lives[self.lives['type'] == 'live'].index.tolist()
    
    def get_channel_info(self, channel_id):
        return chat.ChannelInfo(
            id=channel_id,
            name=self.channels.loc[channel_id]['name'],
            thumbnail_url=self.channels.loc[channel_id]['thumbnail'],
        )

    def get_live_info(self, video_id):
        return chat.VideoInfo(
            id=video_id,
            title=self.lives.loc[video_id]['title'],
            channel=self.get_channel_info(self.lives.loc[video_id]['channel']),
        )
