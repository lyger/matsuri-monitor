import tornado.web

from matsuri_monitor import Supervisor, chat


class MainHandler(tornado.web.RequestHandler):

    async def get(self):
        self.render('main.html')
