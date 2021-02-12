import asyncio
import json
import logging
import re
from urllib.parse import parse_qs, urlparse

import aiohttp
from aiohttp.client_exceptions import ContentTypeError
import tornado.gen
import tornado.ioloop
import tornado.options
from bs4 import BeautifulSoup

from matsuri_monitor import chat, util

logger = logging.getLogger('tornado.general')

INITIAL_CHAT_ENDPOINT_TEMPLATE = 'https://www.youtube.com/live_chat?v={video_id}'
CHAT_ENDPOINT_TEMPLATE = 'https://www.youtube.com/youtubei/v1/live_chat/get_live_chat?key={key}'

REQUEST_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
}

YTCFG_RE = re.compile(r'^\s*ytcfg.set\((.+)\);?$', re.MULTILINE)
YTCFG_ARGS_RE = re.compile(r'^"([A-Z_]+)", (.+)$')

INITIAL_CONTINUATION_PATH = 'contents.liveChatRenderer.continuations.0'
INITIAL_ACTIONS_PATH = 'contents.liveChatRenderer.actions'

CONTINUATION_PATH = 'continuationContents.liveChatContinuation.continuations.0'
ACTIONS_PATH = 'continuationContents.liveChatContinuation.actions'

MESSAGE_PREFIX = 'addChatItemAction.item.liveChatTextMessageRenderer'
SC_PREFIX = 'addLiveChatTickerItemAction.item.liveChatTickerPaidMessageItemRenderer.showItemEndpoint.showLiveChatItemEndpoint.renderer.liveChatPaidMessageRenderer'

AUTHOR_SUBPATH = 'authorName.simpleText'
TEXT_RUNS_SUBPATH = 'message.runs'
TIMESTAMP_SUPBATH = 'timestampUsec'
AMOUNT_SUBPATH = 'purchaseAmountText.simpleText'

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


def traverse_or_none(d, path):
    """Traverse, but return none if not found"""
    try:
        return traverse(d, path)
    except (KeyError, IndexError):
        return None


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

    async def get_initial_chat(self, session: aiohttp.ClientSession, video_id: str) -> dict:
        """Get initial chat JSON object from a continuation token"""

        endpoint = INITIAL_CHAT_ENDPOINT_TEMPLATE.format(video_id=video_id)

        async with session.get(endpoint, headers=REQUEST_HEADERS) as resp:
            soup = BeautifulSoup(await resp.text(), features='lxml')

        chat_obj, key, context = None, None, None

        for script in soup.find_all('script'):
            if 'ytInitialData' in script.text:
                initial_data_str = script.text.split('=', 1)[-1].strip().strip(';')
                chat_obj = json.loads(initial_data_str)
                continue
            
            for args in YTCFG_RE.findall(script.text):
                if args.startswith('{'):
                    args_obj = json.loads(args)
                    if 'INNERTUBE_API_KEY' in args_obj:
                        key = args_obj['INNERTUBE_API_KEY']
                    if 'INNERTUBE_CONTEXT' in args:
                        context = args_obj['INNERTUBE_CONTEXT']
                
                match = YTCFG_ARGS_RE.search(args)
                if match:
                    if match.group(1) == 'INNERTUBE_API_KEY':
                        key = match.group(2)
                    if match.group(1) == 'INNERTUBE_CONTEXT':
                        context = json.loads(match.group(2))

        if chat_obj is None:
            raise RuntimeError('Failed to retrieve initial chat object')

        if key is None:
            raise RuntimeError('Failed to retrieve ytcfg object')

        return chat_obj, key, context

    async def get_next_chat(self, session: aiohttp.ClientSession, continuation_obj: dict, key: str, context: dict) -> dict:
        """Get next chat JSON object from the previous object's continuation object"""

        continuation = None

        # YouTube's API has various "continuationData" types, but any will do if it has a continuation token
        for data in continuation_obj.values():
            if 'continuation' in data:
                continuation = data['continuation']
                break

        if not continuation:
            raise KeyError('No continuation token found')

        endpoint = CHAT_ENDPOINT_TEMPLATE.format(key=key)

        data = {
            'context': context,
            'continuation': continuation,
        }

        async with session.post(endpoint, json=data, headers=REQUEST_HEADERS) as resp:
            try:
                return (await resp.json())
            except ContentTypeError as err:
                logger.exception(f'{err}: {await resp.text()}')
                raise err

    def parse_action(self, action: dict) -> chat.Message:
        start_timestamp = self.info.start_timestamp

        if has_path(action, MESSAGE_PREFIX):
            message_obj = traverse(action, MESSAGE_PREFIX)

            if all(
                has_path(message_obj, pth)
                for pth in [AUTHOR_SUBPATH, TEXT_RUNS_SUBPATH, TIMESTAMP_SUPBATH]
            ):
                author = traverse(message_obj, AUTHOR_SUBPATH)
                text = ''.join(run.get('text', '') for run in traverse(message_obj, TEXT_RUNS_SUBPATH))
                timestamp = float(traverse(message_obj, TIMESTAMP_SUPBATH)) / 1_000_000

                return chat.Message(
                    author=author,
                    text=text,
                    timestamp=timestamp,
                    relative_timestamp=timestamp - start_timestamp,
                )

        elif has_path(action, SC_PREFIX):
            message_obj = traverse(action, SC_PREFIX)

            if all(
                has_path(message_obj, pth)
                for pth in [AUTHOR_SUBPATH, TEXT_RUNS_SUBPATH, TIMESTAMP_SUPBATH, AMOUNT_SUBPATH]
            ):
                author = traverse(message_obj, AUTHOR_SUBPATH)
                text = ''.join(run.get('text', '') for run in traverse(message_obj, TEXT_RUNS_SUBPATH))
                timestamp = float(traverse(message_obj, TIMESTAMP_SUPBATH)) / 1_000_000
                amount = traverse(message_obj, AMOUNT_SUBPATH)

                return chat.SuperChat(
                    author=author,
                    text=text,
                    timestamp=timestamp,
                    relative_timestamp=timestamp - start_timestamp,
                    amount=amount,
                )

        return None

    @util.http_session_method
    async def run(self, session: aiohttp.ClientSession):
        """Monitor process"""
        termination_signals = 0
        termination_cutoff = 10

        for retry in range(INIT_RETRIES):
            try:
                chat_obj, key, context = await self.get_initial_chat(session, self.info.id)
                continuation_obj = traverse(chat_obj, INITIAL_CONTINUATION_PATH)
                actions = traverse_or_none(chat_obj, INITIAL_ACTIONS_PATH)
                break

            except Exception as e:
                if retry == INIT_RETRIES - 1:
                    error_name = type(e).__name__
                    logger.exception(f'Failed to initialize in monitor for video_id={self.info.id} ({error_name})')
                    self._stopped_flag.set()
                    return

                continue

        while True:
            if actions is not None:
                try:
                    new_messages = []

                    for action in actions:
                        message = self.parse_action(action)

                        if message is not None:
                            new_messages.append(message)

                    self.report.add_messages(new_messages)

                except Exception as e:
                    error_name = type(e).__name__
                    logger.exception(f'Error while running monitor for video_id={self.info.id} ({error_name})')
                    self._stopped_flag.set()
                    return

            await tornado.gen.sleep(UPDATE_INTERVAL)

            try:
                chat_obj = await self.get_next_chat(session, continuation_obj, key, context)
                continuation_obj = traverse(chat_obj, CONTINUATION_PATH)
                actions = traverse_or_none(chat_obj, ACTIONS_PATH)

                # On at least one occasion, the monitor has gotten stuck and not terminated
                # I'm not sure why, but this should ensure the monitor quits eventually
                if self._terminate_flag.is_set():
                    termination_signals += 1

                if termination_signals >= termination_cutoff:
                    logger.warning(
                        f'Stopping monitor {termination_cutoff} iterations after termination '
                        f'for video_id={self.info.id}'
                    )
                    break

            except (KeyError, json.JSONDecodeError) as e:
                logger.exception(f'Could not fetch more chat for video_id={self.info.id}: {type(e).__name__}')
                break

            except Exception as e:
                error_name = type(e).__name__
                logger.exception(f'Error while fetching continuation for video_id={self.info.id} ({error_name})')
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
