import json
import logging
import multiprocessing as mp
import queue
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import requests
import tornado.ioloop
import tornado.options
from bs4 import BeautifulSoup

from matsuri_monitor import chat

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
        super().__init__()
        self.info = info
        self.report = report
        self._terminate_flag = mp.Event()
        self._stopped_flag = mp.Event()

    @property
    def is_running(self):
        return not self._stopped_flag.is_set()

    def get_live_details(self):
        """Get live details of the live this monitor is monitoring"""

        params = {
            'part': 'liveStreamingDetails',
            'id': self.info.id,
            'key': tornado.options.options.api_key,
        }

        resp = requests.get(VIDEO_API_ENDPOINT, params=params)

        items = resp.json()['items']

        if len(items) < 1:
            raise RuntimeError('Could not get live broadcast info')

        return items[0]['liveStreamingDetails']

    def get_initial_chat(self, continuation: str) -> dict:
        """Get initial chat JSON object from a continuation token"""

        endpoint = INITIAL_CHAT_ENDPOINT_TEMPLATE.format(continuation=continuation)

        soup = BeautifulSoup(requests.get(endpoint, headers=REQUEST_HEADERS).text, features='lxml')

        for script in soup.find_all('script'):
            if 'ytInitialData' in script.text:
                break

        initial_data_str = script.text.split('=', 1)[-1].strip().strip(';')

        return json.loads(initial_data_str)

    def get_next_chat(self, continuation_obj: dict) -> dict:
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

        resp = requests.get(endpoint, headers=REQUEST_HEADERS)

        return resp.json()['response']

    def run(self):
        """Monitor process"""

        live_info = self.get_live_details()

        start_timestamp = datetime.fromisoformat(
            live_info['actualStartTime'].rstrip('zZ')
        ).replace(tzinfo=timezone.utc).timestamp()

        self.info.start_timestamp = start_timestamp

        for retry in range(INIT_RETRIES):
            try:
                soup = BeautifulSoup(requests.get(self.info.url).text, features='lxml')
                chat_url = soup.find(id='live-chat-iframe').attrs['src']

                continuation = parse_qs(urlparse(chat_url).query)['continuation'][0]

                chat_obj = self.get_initial_chat(continuation)
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

            time.sleep(UPDATE_INTERVAL)

            try:
                continuation_obj = traverse(chat_obj, CONTINUATION_PATH)
                chat_obj = self.get_next_chat(continuation_obj)
            except (KeyError, json.JSONDecodeError):
                logger.info(f'Could not fetch more chat for video_id={self.info.id}')
                break

        self._terminate_flag.wait()

        logger.info(f'Serializing report for video_id={self.info.id}')
        self.report.save_and_finalize()
        self._stopped_flag.set()

    def start(self, current_ioloop: tornado.ioloop.IOLoop = None):
        """Spawn monitor process in executor on the current IOLoop"""

        if current_ioloop is None:
            current_ioloop = tornado.ioloop.IOLoop.current()
        self._running = True

        logger.info(f'Begin monitoring video_id={self.info.id}')
        current_ioloop.run_in_executor(None, self.run)

    def terminate(self):
        """Signal this process to terminate"""
        logger.info(f'Received terminate signal for video_id={self.info.id}')
        self._terminate_flag.set()
