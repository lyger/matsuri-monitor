import requests
import multiprocessing as mp
import pandas as pd

CHANNEL_ENDPOINT = 'https://storage.googleapis.com/vthell-data/channels.json'
LIVE_ENDPOINT = 'https://storage.googleapis.com/vthell-data/live.json'
VIDEO_ENDPOINT = 'https://storage.googleapis.com/vthell-data/videos.json'


class Jetri:

    def __init__(self):
        channels = requests.get(CHANNEL_ENDPOINT).json()['channels']
        self._lock = mp.Lock()
        self.channels = pd.DataFrame.from_records(channels, index='id')
        self.lives = pd.DataFrame(index=pd.Series(name='id'), columns=['title', 'type', 'startTime', 'channel'])
        self.videos = pd.DataFrame(index=pd.Series(name='id'), columns=['channel', 'title', 'time'])

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

            self.videos = pd.DataFrame.from_records(
                map(lambda t: dict(id=t[0], **t[1]), videos.items()), index='id'
            )
