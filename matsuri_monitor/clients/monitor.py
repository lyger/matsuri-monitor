import json
import multiprocessing as mp
import queue
import requests
import time

from bs4 import BeautifulSoup
from flask import current_app
from urllib.parse import urlparse, parse_qs

from matsuri_monitor import chat, datashare

VIDEO_API_ENDPOINT = 'https://www.googleapis.com/youtube/v3/videos'

VIDEO_URL_TEMPLATE = 'https://www.youtube.com/watch?v={video_id}'
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

def get_paths(d, prefix=''):
    if isinstance(d, dict):
        gen = d.items()
    elif isinstance(d, list):
        gen = enumerate(d)
    else:
        raise ValueError()
    for k, v in gen:
        if isinstance(v, dict) or isinstance(v, list):
            yield from get_paths(v, f'{prefix}{k}.')
        else:
            yield f'{prefix}{k}'

def has_path(d, path):
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
    for k in path.split('.'):
        if k.isdigit():
            k = int(k)
        d = d[k]
    return d


class Monitor(mp.Process):

    def __init__(self, video_id):
        self.events = mp.Queue()
        self.conn_in, self.conn_out = mp.Pipe()
        self.video_id = video_id
        self.video_url = VIDEO_URL_TEMPLATE.format(video_id=video_id)

    def get_live_info(self):
        params = {
            'part': 'liveStreamingDetails',
            'id': self.video_id,
            'key': datashare.api_key.value,
        }

        resp = requests.get(VIDEO_API_ENDPOINT, params=params)

        items = resp.json()['items']

        if len(items) < 1:
            raise ValueError('TODO')

    def get_initial_chat(self, continuation):
        endpoint = INITIAL_CHAT_ENDPOINT_TEMPLATE.format(continuation=continuation)

        soup = BeautifulSoup(requests.get(endpoint, headers=REQUEST_HEADERS).text, features='lxml')

        for script in soup.find_all('script'):
            if 'ytInitialData' in script.text:
                break
        
        initial_data_str = script.text.split('=', 1)[-1].strip().strip(';')

        return json.loads(initial_data_str)

    def get_next_chat(self, continuation_obj):
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
        soup = BeautifulSoup(requests.get(self.video_url).text, features='lxml')
        chat_url = soup.find(id='live-chat-iframe').attrs['src']

        continuation = parse_qs(urlparse(chat_url).query)['continuation'][0]

        for retry in range(INIT_RETRIES):
            try:
                chat_obj = self.get_initial_chat(continuation)
                break
            except:
                continue

        while True:
            if has_path(chat_obj, ACTIONS_PATH):
                actions = traverse(chat_obj, ACTIONS_PATH)

                for action in actions:
                    if not all(
                        has_path(action, pth)
                        for pth in [AUTHOR_SUBPATH, TEXT_RUNS_SUBPATH, TIMESTAMP_SUPBATH]
                    ):
                        continue

                    author = traverse(action, AUTHOR_SUBPATH)
                    text = ''.join(run.get('text', '') for run in traverse(action, TEXT_RUNS_SUBPATH))
                    timestamp = float(traverse(action, TIMESTAMP_SUPBATH)) / 1_000_000

                    message = chat.Message(author=author, text=text, timestamp=timestamp)

                    self.events.put_nowait(message)

                    print(message)
            
            time.sleep(UPDATE_INTERVAL)

            try:
                continuation_obj = traverse(chat_obj, CONTINUATION_PATH)
                chat_obj = self.get_next_chat(continuation_obj)
            except KeyError:
                break
    
    def iter_events(self):
        while True:
            try:
                event = self.events.get_nowait()
                yield event
            except queue.Empty:
                break
