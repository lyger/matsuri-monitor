import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import aiohttp
import tornado.gen
import tornado.ioloop
import tornado.options
from bs4 import BeautifulSoup

from matsuri_monitor import chat, util

tornado.options.define('api-key', type=str, help='YouTube API key')

logger = logging.getLogger('tornado.general')

VIDEO_API_ENDPOINT = 'https://www.googleapis.com/youtube/v3/videos'

INITIAL_CHAT_ENDPOINT_TEMPLATE = 'https://www.youtube.com/live_chat/get_live_chat?continuation={continuation}'
CHAT_ENDPOINT_TEMPLATE = 'https://www.youtube.com/live_chat/get_live_chat?continuation={continuation}&pbj=1'

REQUEST_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
}

CONTINUATION_PATH = 'continuationContents.liveChatContinuation.continuations.0'
ACTIONS_PATH = 'continuationContents.liveChatContinuation.actions'

AUTHOR_SUBPATH = 'addChatItemAction.item.liveChatTextMessageRenderer.authorName.simpleText'
TEXT_RUNS_SUBPATH = 'addChatItemAction.item.liveChatTextMessageRenderer.message.runs'
TIMESTAMP_SUPBATH = 'addChatItemAction.item.liveChatTextMessageRenderer.timestampUsec'

INIT_RETRIES = 5
UPDATE_INTERVAL = 1

def has_path(d, path):
    """Check if a dot-separated path is in the given nested dict/list"""
    for k in path.split('.'):
        if k.isdigit():
            k = int(k)
            if k >= len(d):
                return False
        else:
            if k not in d:
                return False
        d = d[k]
    return True


def traverse(d, path):
    """Return the value at the given path from the given nested dict/list"""
    for k in path.split('.'):
        if k.isdigit():
            k = int(k)
        d = d[k]
    return d


class Monitor:

    def __init__(self, info: chat.VideoInfo, report: chat.LiveReport):
        """init

        Parameters
        ----------
        info
            VideoInfo for the video to monitor
        report
            LiveReport to write chat messages to
        """
        self.info = info
        self.report = report
        self._terminate_flag = asyncio.Event()
        self._stopped_flag = asyncio.Event()

    @property
    def is_running(self):
        return not self._stopped_flag.is_set()

    async def get_live_details(self, session: aiohttp.ClientSession):
        """Get live details of the live this monitor is monitoring"""

        params = {
            'part': 'liveStreamingDetails',
            'id': self.info.id,
            'key': tornado.options.options.api_key,
        }

        async with session.get(VIDEO_API_ENDPOINT, params=params) as resp:
            items = (await resp.json())['items']

        if len(items) < 1:
            raise RuntimeError('Could not get live broadcast info')

        return items[0]['liveStreamingDetails']

    async def get_initial_chat(self, session: aiohttp.ClientSession, continuation: str) -> dict:
        """Get initial chat JSON object from a continuation token"""

        endpoint = INITIAL_CHAT_ENDPOINT_TEMPLATE.format(continuation=continuation)

        async with session.get(endpoint, headers=REQUEST_HEADERS) as resp:
            soup = BeautifulSoup(await resp.text(), features='lxml')

        for script in soup.find_all('script'):
            if 'ytInitialData' in script.text:
                break

        initial_data_str = script.text.split('=', 1)[-1].strip().strip(';')

        return json.loads(initial_data_str)

    async def get_next_chat(self, session: aiohttp.ClientSession, continuation_obj: dict) -> dict:
        """Get next chat JSON object from the previous object's continuation object"""

        continuation = None

        # YouTube's API has various "continuationData" types, but any will do if it has a continuation token
        for data in continuation_obj.values():
            if 'continuation' in data:
                continuation = data['continuation']
                break

        if not continuation:
            raise KeyError('No continuation token found')

        endpoint = CHAT_ENDPOINT_TEMPLATE.format(continuation=continuation)

        async with session.get(endpoint, headers=REQUEST_HEADERS) as resp:
            return (await resp.json())['response']

    @util.http_session_method
    async def run(self, session: aiohttp.ClientSession):
        """Monitor process"""

        live_info = await self.get_live_details(session)

        start_timestamp = datetime.fromisoformat(
            live_info['actualStartTime'].rstrip('zZ')
        ).replace(tzinfo=timezone.utc).timestamp()

        self.info.start_timestamp = start_timestamp

        for retry in range(INIT_RETRIES):
            try:
                async with session.get(self.info.url) as resp:
                    soup = BeautifulSoup(await resp.text(), features='lxml')

                chat_url = soup.find(id='live-chat-iframe').attrs['src']

                continuation = parse_qs(urlparse(chat_url).query)['continuation'][0]

                chat_obj = await self.get_initial_chat(session, continuation)
                break

            except Exception as e:
                if retry == INIT_RETRIES - 1:
                    error_name = type(e).__name__
                    logger.exception(f'Failed to initialize in monitor for video_id={self.info.id} ({error_name})')
                    self._stopped_flag.set()
                    return

                continue

        while True:
            if has_path(chat_obj, ACTIONS_PATH):
                try:
                    actions = traverse(chat_obj, ACTIONS_PATH)

                    new_messages = []

                    for action in actions:
                        if not all(
                            has_path(action, pth)
                            for pth in [AUTHOR_SUBPATH, TEXT_RUNS_SUBPATH, TIMESTAMP_SUPBATH]
                        ):
                            continue

                        author = traverse(action, AUTHOR_SUBPATH)
                        text = ''.join(run.get('text', '') for run in traverse(action, TEXT_RUNS_SUBPATH))
                        timestamp = float(traverse(action, TIMESTAMP_SUPBATH)) / 1_000_000

                        message = chat.Message(
                            author=author,
                            text=text,
                            timestamp=timestamp,
                            relative_timestamp=timestamp - start_timestamp,
                        )

                        new_messages.append(message)

                    self.report.add_messages(new_messages)

                except Exception as e:
                    error_name = type(e).__name__
                    logger.exception(f'Error while running monitor for video_id={self.info.id} ({error_name})')
                    self._stopped_flag.set()
                    return

            await tornado.gen.sleep(UPDATE_INTERVAL)

            try:
                continuation_obj = traverse(chat_obj, CONTINUATION_PATH)
                chat_obj = await self.get_next_chat(session, continuation_obj)
            except (KeyError, json.JSONDecodeError):
                logger.info(f'Could not fetch more chat for video_id={self.info.id}')
                break

        await self._terminate_flag.wait()

        logger.info(f'Serializing report for video_id={self.info.id}')
        self.report.save()
        self._stopped_flag.set()

        logger.info(f'Monitor finished for video_id={self.info.id}')

    def start(self, current_ioloop: tornado.ioloop.IOLoop = None):
        """Spawn monitor process in executor on the current IOLoop"""

        if current_ioloop is None:
            current_ioloop = tornado.ioloop.IOLoop.current()
        self._running = True

        logger.info(f'Begin monitoring video_id={self.info.id}')
        current_ioloop.add_callback(self.run)

    def terminate(self):
        """Signal this process to terminate"""
        logger.info(f'Received terminate signal for video_id={self.info.id}')
        self._terminate_flag.set()
