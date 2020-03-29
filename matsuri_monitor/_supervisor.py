from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Dict, List

import tornado.ioloop
from cachetools import TTLCache, cached

from matsuri_monitor import chat, clients

HISTORY_CUTOFF = timedelta(days=7).total_seconds()


class Supervisor:

    def __init__(self, interval: float):
        super().__init__()
        self.interval = interval
        self.jetri = clients.Jetri()
        self.live_monitors: Dict[str, Monitor] = OrderedDict()
        self.archive_reports: List[chat.LiveReport] = []
        self.groupers = chat.Grouper.load()

    def update(self, current_ioloop: tornado.ioloop.IOLoop = None):
        """Periodic update of overall app state

        Checks for new lives, prunes old ones and adds them to the archives, and refreshes groupers

        Parameters
        ----------
        current_ioloop
            The IOLoop to use for this process and when spawning monitor processes
        """
        if current_ioloop is None:
            current_ioloop = tornado.ioloop.IOLoop.current()

        new_groupers = chat.Grouper.load()
        if new_groupers != self.groupers:
            for monitor in self.live_monitors.values():
                monitor.report.set_groupers(new_groupers)
            self.groupers = new_groupers

        self.jetri.update()
        for video_id in self.jetri.currently_live:
            if video_id not in self.live_monitors:
                info = self.jetri.get_live_info(video_id)

                report = chat.LiveReport(info)
                report.set_groupers(self.groupers)

                monitor = clients.Monitor(info, report)
                monitor.start(current_ioloop)

                self.live_monitors[video_id] = monitor

        to_delete = []

        for video_id, monitor in self.live_monitors.items():
            if not monitor.is_running:
                to_delete.append(video_id)

                report = monitor.report
                report.finalize()
                if len(report) > 0:
                    self.archive_reports.insert(0, report)
        
        for video_id in to_delete:
            del self.live_monitors[video_id]

        self.prune()

    def prune(self):
        timestamp_now = datetime.utcnow().timestamp()
        cutoff = timestamp_now - HISTORY_CUTOFF
        pruned_reports = list(filter(lambda r: r.info.start_timestamp > cutoff, self.archive_reports))

        self.archive_reports = pruned_reports

    @cached(TTLCache(1, 5))
    def live_json(self) -> Dict:
        return {'reports': [monitor.report.json() for monitor in self.live_monitors.values()]}
    
    @cached(TTLCache(1, 30))
    def archive_json(self) -> Dict:
        return {'reports': [report.json() for report in self.archive_reports]}
    
    def get_scheduler(self) -> tornado.ioloop.PeriodicCallback:
        def update_async():
            current_ioloop = tornado.ioloop.IOLoop.current()
            current_ioloop.run_in_executor(None, self.update, current_ioloop)

        return tornado.ioloop.PeriodicCallback(update_async, self.interval * 1000)
