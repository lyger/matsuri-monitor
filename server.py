from pathlib import Path

import tornado
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from matsuri_monitor import Supervisor, handlers

tornado.options.define('port', default=8080, type=int, help='Run on the given port')
tornado.options.define('debug', default=False, type=bool, help='Run in debug mode')
tornado.options.define('interval', default=300, type=float, help='Seconds between updates')


def main():
    """Create app and start server"""
    supervisor = Supervisor(tornado.options.options.interval)

    static_path = Path(__file__).parent.absolute() / 'matsuri_monitor' / 'static'
    static_url_prefix = r'/_monitor/static/'

    print(static_path)

    server = tornado.httpserver.HTTPServer(
        tornado.web.Application(
            [
                (r'/_monitor', handlers.MainHandler),
                (r'/_monitor/live.json', handlers.APIHandler, {'json_source': supervisor.live_json}),
                (r'/_monitor/archive.json', handlers.APIHandler, {'json_source': supervisor.archive_json}),
            ],
            debug=tornado.options.options.debug,
            static_path=static_path,
            static_url_prefix=static_url_prefix,
        )
    )

    if tornado.options.options.debug:
        server.listen(tornado.options.options.port)
    else:
        server.bind(tornado.options.options.port)
        server.start(1)

    current_ioloop = tornado.ioloop.IOLoop.current()

    supervisor.update()
    supervisor.get_scheduler(current_ioloop).start()

    current_ioloop.start()


if __name__ == '__main__':
    tornado.options.parse_command_line()
    main()
