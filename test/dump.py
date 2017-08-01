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

def _app_publish(client):
    def _app_publisher(**kwargs):
        client.on_message(client, client.userdata, type('Message',(),kwargs))
    return _app_publisher

def _dump(data, publisher):
#    for dataEntry in tqdm(data, "Send messages"):
    for dataEntry in tqdm(data, "Send messages"):
        publisher(topic = "/home/meterDigitizer/%(id)s", payload = json.dumps(dataEntry, cls=DateTimeEncoder).encode())

class MocMQTT(object):
    def __init__(self, userdata, *args, **kwargs):
        self.userdata = userdata
        self.on_message = None
        self.on_connect = None
        self.on_disconnect = None

    def connect(self, *args, **kwargs):
        if self.on_connect:
            self.on_connect(self, self.userdata, 0, 0)
        return True
    connect_srv = connect
    connect_async = connect

    def subscribe(self, *args, **kwargs):
        pass

    def loop_start(self, *args, **kwargs):
        pass

    def loop_stop(self, *args, **kwargs):
        pass

    def disconnect(self, *args, **kwargs):
        if self.on_disconnect:
            self.on_disconnect(self, self.userdata, 0)

def _dump_db(data, output):
    import sys,  os, sqlite3
    from os.path import realpath
    sys.path.insert(0, realpath("../"))
    if 'FLASK_DEBUG' in os.environ:
        os.environ.pop('FLASK_DEBUG')
    import config
    from app import MeterDigitizer

    db_name = "file:dump_db?cache=shared&mode=memory"
    db = sqlite3.connect(db_name, uri=True)

    config.parse_args(["--db",db_name])
    app = MeterDigitizer(mqtt_class=MocMQTT)
    _dump(data, _app_publish(app.mqtt_client))
    del app
    with output:
        print("""\
BEGIN TRANSACTION;
DROP TABLE IF EXISTS "values";
DROP TABLE IF EXISTS "sensors";
COMMIT;""", file=output)
        for line in db.iterdump():
            print(line, file=output)
    #save data to sql

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser();
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--db", type=argparse.FileType('w'))
    group.add_argument("--mqtt", action="store_true")
    args = parser.parse_args()
    data = generate()
    if args.db:
        _dump_db(data, args.db)
    if args.mqtt:
        _dump(data, _mqtt_publisher)
