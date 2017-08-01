#!/usr/bin/env python3

from generator import generate
from tqdm import tqdm
import json
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        from datetime import date
        if(isinstance(o, date)):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

def _mqtt_publisher(**kwargs):
    import paho.mqtt.publish as publish
    from time import sleep
    publish.single(**kwargs)
    sleep(0.100)

def _app_publish(client, app):
    def _app_publisher(**kwargs):
        app.mqtt_on_message(client, app.md, type('Message',(),kwargs))
    return _app_publisher

def _dump(data, publisher):
#    for dataEntry in tqdm(data, "Send messages"):
    for dataEntry in tqdm(data, "Send messages"):
        publisher(topic = "/home/meterDigitizer/%(id)s", payload = json.dumps(dataEntry, cls=DateTimeEncoder).encode())

def _dump_db(data):
    import sys
    from os.path import realpath
    sys.path.insert(0, realpath("../"))
    import app
    class Client(object):
        def subscribe(self, *args, **kwargs):
            pass
    client = Client()
    app.mqtt_on_connect(client, app.md, None, 0)
    _dump(data, _app_publish(client, app))
    app.mqtt_on_disconnect(client, app.md, 0)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser();
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--db", action="store_true")
    group.add_argument("--mqtt", action="store_true")
    args = parser.parse_args()
    data = generate()
    if args.db:
        _dump_db(data)
    if args.mqtt:
        _dump(data, _mqtt_publisher)
