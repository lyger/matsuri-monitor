import logging
from collections import OrderedDict
from typing import Dict, List

import tornado.gen
import tornado.ioloop
import tornado.options
from cachetools import TTLCache, cached

from matsuri_monitor import chat, clients

tornado.options.define('history-days', default=7, type=int, help='Number of days of history to save')

logger = logging.getLogger('tornado.general')


class Supervisor:

    def __init__(self, interval: float):
        """init

        Parameters
        ----------
        interval
            Update interval in seconds
        """
        super().__init__()
        self.interval = interval
        self.api = clients.HoloDex()
        self.live_monitors: Dict[str, clients.Monitor] = OrderedDict()
        self.groupers = chat.Grouper.load()
        tornado.options.options.archives_dir.mkdir(exist_ok=True)

    async def update(self, current_ioloop: tornado.ioloop.IOLoop = None):
        """Periodic update of overall app state

        Checks for new lives, prunes old ones and adds them to the archives, and refreshes groupers

        Parameters
        ----------
        current_ioloop
            The IOLoop to use for this process and when spawning monitor processes
        """
        if current_ioloop is None:
            current_ioloop = tornado.ioloop.IOLoop.current()

        logger.info('[Begin supervisor update]')

        # Refresh groupers
        new_groupers = chat.Grouper.load()
        if new_groupers != self.groupers:
            for monitor in self.live_monitors.values():
                monitor.report.set_groupers(new_groupers)
            self.groupers = new_groupers

        # Clean up terminated monitors (including those that terminated with an error)
        to_delete = []

        for video_id, monitor in self.live_monitors.items():
            if not monitor.is_running:
                to_delete.append(video_id)

        logger.info(f'Removing {len(to_delete)} stopped monitors: {to_delete}')

        for video_id in to_delete:
            self.live_monitors[video_id].terminate()
            del self.live_monitors[video_id]

        # Refresh currently live list and find lives to start and terminate
        await self.api.update()

        currently_live = set(self.api.currently_live)
        currently_monitored = set(self.live_monitors.keys())

        new_lives = currently_live - currently_monitored
        stopped_lives = currently_monitored - currently_live

        # Start new lives
        for video_id in new_lives:
            info = self.api.get_live_info(video_id)

            report = chat.LiveReport(info)
            report.set_groupers(self.groupers)

            monitor = clients.Monitor(info, report)
            monitor.start(current_ioloop)

            self.live_monitors[video_id] = monitor

        logger.info(f'Started {len(new_lives)} new monitors')

        # Send terminate signal to finished lives and move reports to archives
        for video_id in stopped_lives:
            monitor = self.live_monitors[video_id]
            monitor.terminate()

        logger.info(f'Terminated {len(stopped_lives)} monitors: {list(stopped_lives)}')

        logger.info('[End supervisor update]')

    @cached(TTLCache(1, 5))
    def live_json(self) -> dict:
        """JSON object containing reports of all currently live streams"""
        return {'reports': [monitor.report.json() for monitor in self.live_monitors.values() if monitor.is_running]}

    def start(self, current_ioloop: tornado.ioloop.IOLoop):
        """Begin update loop"""
        async def update_loop():
            while True:
                try:
                    await self.update(current_ioloop)
                except Exception as e:
                    error_name = type(e).__name__
                    logger.exception(f'Exception in supervisor update ({error_name})')
                await tornado.gen.sleep(self.interval)

        current_ioloop.add_callback(update_loop)
