#!/usr/bin/env python3

from generator import generate
from tqdm import tqdm
import json
from datetime import timedelta
import re

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
    sys.path.insert(0, os.path.normpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__), "..")
        ))
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

def get_timedelta(value):
    match = re.match("^(\d+)(s|m|h|d)*$", value)
    if not match:
        raise ValueError("Value should be in format (\d+)(s|m|h|d)*")
    converter = {
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days",
        None: "days"
        }
    return timedelta(**{converter[match.group(2)]: int(match.group(1))})



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter);
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--db", type=argparse.FileType('w'),
                       help="Output to SQL file")
    group.add_argument("--mqtt", action="store_true",
                       help="Output to MQTT brokker")
    parser.add_argument("--sensors", "-s", default=6, type=int,
                        help="Number of generated sensors")
    parser.add_argument("--timespan", "-t", default="1d", type=get_timedelta,
                        help="Generated duration")
    parser.add_argument("--length", "-l", default="20m", type=get_timedelta,
                        help="Range of random length of series")
    parser.add_argument("--offset", "-o", default="10h", type=get_timedelta,
                        help="Range of random time between series")
    args = parser.parse_args()
    data = generate(sensors = args.sensors, timespan=args.timespan, length=args.length, offset=args.offset)
    if args.db:
        _dump_db(data, args.db)
    if args.mqtt:
        _dump(data, _mqtt_publisher)
