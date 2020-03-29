from typing import Callable

import tornado.web


class APIHandler(tornado.web.RequestHandler):

    def initialize(self, json_source: Callable):
        self.json_source = json_source

    async def get(self):
        self.write(self.json_source())
