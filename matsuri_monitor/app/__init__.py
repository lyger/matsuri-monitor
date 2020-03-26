from flask import Flask

from matsuri_monitor import datashare

def create_app():
    app = Flask(__name__)
    app.config.from_object('matsuri_monitor.app.config')

    with datashare.api_key.get_lock():
        datashare.api_key.value = app.config['API_KEY']
