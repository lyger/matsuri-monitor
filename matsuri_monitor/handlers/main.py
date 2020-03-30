import tornado.web

from matsuri_monitor import Supervisor, chat


class MainHandler(tornado.web.RequestHandler):

    async def get(self):
        """GET /_monitor"""
        self.render('main.html')
