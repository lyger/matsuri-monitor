import multiprocessing as mp
from datetime import datetime, timezone

import aiohttp
import pandas as pd

from matsuri_monitor import chat, util

CHANNEL_ENDPOINT = "https://holodex.net/api/v2/channels"
LIVE_ENDPOINT = "https://holodex.net/api/v2/live"
WATCHED_ORGS = ["Hololive", "Nijisanji", "VSpo", "774inc"]


class HoloDex:
    def __init__(self):
        self._lock = mp.Lock()
        self.lives = pd.DataFrame(
            index=pd.Series(name="id"), columns=["title", "start", "channel"]
        )

    async def retrieve_channels(self, session: aiohttp.ClientSession):
        channels = []

        for org in WATCHED_ORGS:
            offset = 0
            while True:
                params = {"offset": offset, "limit": 50, "type": "vtuber", "org": org}
                async with session.get(CHANNEL_ENDPOINT, params=params) as resp:
                    new_channels = await resp.json()

                if not new_channels:
                    break

                for channel in new_channels:
                    channel["org"] = org

                channels += new_channels
                offset += 50

        self.channels = pd.DataFrame.from_records(channels, index="id")

    @util.http_session_method
    async def update(self, session: aiohttp.ClientSession):
        """Re-accesses jetri endpoints and updates active lives"""
        if not hasattr(self, "channels"):
            await self.retrieve_channels(session)

        lives = []
        for org in WATCHED_ORGS:
            offset = 0
            while True:
                params = {"offset": offset, "limit": 50, "status": "live", "org": org}
                async with session.get(LIVE_ENDPOINT, params=params) as resp:
                    new_lives = await resp.json()

                if not new_lives:
                    break

                lives += new_lives
                offset += 50

        include_cols = ["id", "title", "live_start", "channel"]

        lives_records = [
            {
                "id": lv["id"],
                "title": lv["title"],
                "live_start": lv["start_actual"],
                "channel": lv["channel"]["id"],
            }
            for lv in lives
            if "start_actual" in lv
        ]

        if len(lives_records) > 0:
            lives_df = pd.DataFrame.from_records(
                lives_records,
                index="id",
                columns=include_cols,
            )
            lives_df = lives_df[~lives_df.index.duplicated(keep="first")]
        else:
            lives_df = pd.DataFrame(
                index=pd.Series(name="id", dtype=str),
                columns=include_cols[1:],
            )

        with self._lock:
            self.lives = lives_df

    @property
    def currently_live(self):
        """Returns IDs of currently live streams"""
        return self.lives.index.tolist()

    def get_channel_info(self, channel_id: str):
        """Returns a ChannelInfo object for the given channel ID"""
        channel_row = self.channels.loc[channel_id]
        return chat.ChannelInfo(
            id=channel_id,
            name=channel_row["name"],
            thumbnail_url=channel_row["photo"],
            org=channel_row["org"],
        )

    def get_live_info(self, video_id: str):
        """Returns a VideoInfo object for the given video ID"""
        row = self.lives.loc[video_id]
        return chat.VideoInfo(
            id=video_id,
            title=row["title"],
            channel=self.get_channel_info(row["channel"]),
            start_timestamp=datetime.fromisoformat(row["live_start"].rstrip("zZ"))
            .replace(tzinfo=timezone.utc)
            .timestamp(),
        )
