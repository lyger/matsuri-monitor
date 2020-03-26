import multiprocessing as mp
from matsuri_monitor import clients

class Supervisor(mp.Process):

    def __init__(self, interval):
        self.interval = interval
        self.event_listeners = []
        self.active_lives = {}
        self.jetri = clients.Jetri()

    def add_listener(self, trigger, handler):
        self.event_listeners.append((trigger, handler))

    def run(self):
        pass

    def handle_messages(self, messages):
        # TODO: This is a dummy method to demonstrate the desired logic
        for message in messages:
            for trigger, handler in self.event_listeners:
                if trigger(message):
                    handler(message)
