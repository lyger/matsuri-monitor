import multiprocessing as mp
from datetime import datetime, timezone

import aiohttp
import pandas as pd

from matsuri_monitor import chat, util

CHANNEL_ENDPOINT = 'https://api.holotools.app/v1/channels'
LIVE_ENDPOINT = 'https://api.holotools.app/v1/live'


class Jetri:

    def __init__(self):
        self._lock = mp.Lock()
        self.lives = pd.DataFrame(index=pd.Series(name='id'), columns=['title', 'start', 'channel'])

    async def retrieve_channels(self, session: aiohttp.ClientSession):
        channels = []

        for offset in range(0, 200, 50):
            async with session.get(CHANNEL_ENDPOINT, params={'offset': offset, 'limit': 50}) as resp:
                new_channels = (await resp.json())['channels']
            
            if len(new_channels) == 0:
                break

            channels += new_channels

        self.channels = pd.DataFrame.from_records(channels, index='id')

    @util.http_session_method
    async def update(self, session: aiohttp.ClientSession):
        """Re-accesses jetri endpoints and updates active lives"""
        if not hasattr(self, 'channels'):
            await self.retrieve_channels(session)

        async with session.get(LIVE_ENDPOINT) as resp:
            lives = await resp.json()

        include_cols = ['id', 'title', 'yt_video_key', 'live_start', 'channel']

        lives_records = list(filter(
            lambda lv: lv['yt_video_key'] is not None and lv['live_start'] is not None,
            lives['live'],
        ))
        lives_records = list(map(lambda lv: {key: lv[key] for key in include_cols}, lives_records))

        for lv in lives_records:
            lv['channel'] = lv['channel']['id']

        if len(lives_records) > 0:
            lives_df = pd.DataFrame.from_records(
                lives_records,
                index='yt_video_key',
                columns=include_cols,
            )
        else:
            lives_df = pd.DataFrame(
                index=pd.Series(name='yt_video_key', dtype=str),
                columns=include_cols,
            )

        with self._lock:
            self.lives = lives_df

    @property
    def currently_live(self):
        """Returns IDs of currently live streams"""
        return self.lives.index.tolist()

    def get_channel_info(self, channel_id: str):
        """Returns a ChannelInfo object for the given channel ID"""
        return chat.ChannelInfo(
            id=channel_id,
            name=self.channels.loc[channel_id]['name'],
            thumbnail_url=self.channels.loc[channel_id]['photo'],
        )

    def get_live_info(self, video_id: str):
        """Returns a VideoInfo object for the given video ID"""
        row = self.lives.loc[video_id]
        return chat.VideoInfo(
            id=video_id,
            title=row['title'],
            channel=self.get_channel_info(row['channel']),
            start_timestamp=datetime.fromisoformat(
                row['live_start'].rstrip('zZ')
            ).replace(tzinfo=timezone.utc).timestamp()
        )
