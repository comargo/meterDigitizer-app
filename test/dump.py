#!/usr/bin/env python3

from generator import generate
from tqdm import trange, tqdm

def _dump_sql(data, filename):
    import sqlite3
    con = sqlite3.connect(":memory:")
    with con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS "sensors" (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
            );
        CREATE TABLE IF NOT EXISTS "values" (
            "timestamp" TIMESTAMP NOT NULL,
            "id" INTEGER NOT NULL,
            "value" REAL NOT NULL,
            FOREIGN KEY(id) REFERENCES sensors(id),
            PRIMARY KEY(id, timestamp)
            );
        """)
    knownSensors = [];
    with con:
        for dataEntry in tqdm(data, "Fill database"):
            if dataEntry["id"] not in knownSensors:
                con.execute("""INSERT INTO "sensors" VALUES(:id, :name)""", dataEntry)
                knownSensors.append(dataEntry["id"])
            con.execute("""INSERT INTO "values" VALUES (datetime(:timestamp, "localtime"), :id, :value)""", dataEntry)
    with open(filename, "w") as f:
        for line in tqdm(con.iterdump(), desc = "Save database"):
            print(line, file=f)


def _dump_mqtt(data):
    import paho.mqtt.publish as publish
    from time import sleep
    import json
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, o):
            from datetime import date
            if(isinstance(o, date)):
                return o.isoformat()
            return json.JSONEncoder.default(self, o)

    for dataEntry in tqdm(data, "Send messages"):
        publish.single("/home/meterDigitizer/%(id)s", json.dumps(dataEntry, cls=DateTimeEncoder))
        sleep(0.100)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser();
    parser.add_argument("--sql")
    parser.add_argument("--mqtt", action="store_true")
    args = parser.parse_args()
    data = generate()
    if args.sql:
        _dump_sql(data, args.sql)
    if args.mqtt:
        _dump_mqtt(data)
