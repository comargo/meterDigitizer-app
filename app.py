#!/usr/bin/env python3

import os, sys, datetime, time
import sqlite3
import paho.mqtt.client as mqtt
import configparser
from flask import Flask, render_template, request, json
from plotly.offline import offline
from plotly.graph_objs import Scatter
#from os.path import basename

from diffirentiate_data import differentiate_data

def mqtt_on_connect(client, userdata, flags, rc):
    return userdata.mqtt_on_connect(client, flags, rc)

def mqtt_on_message(client, userdata, msg):
    return userdata.mqtt_on_message(client, msg)

def mqtt_on_disconnect(client, userdata, rc):
    return userdata.mqtt_on_disconnect(client, rc)

defaultConfig = {
        "Server" : {
            "Port": 8080,
            "Host": "0.0.0.0",
        },
        "MQTT" : {
            "Server": "localhost",
            "keepalive": 60,
        },
        "DB" : {
            "engine" : "sqlite3",
            "database" : "db/meterDigitizer.sqlite3"
        }
    }

config = configparser.ConfigParser()
config.read_dict(defaultConfig)
config.read("config/meterDigitizer.ini")

class MeterDigitizer:
    mqtt = type('',(),dict(
        client = mqtt.Client(),
        con = None,
        knownSensors = []
    ))()
    web_db_connection = None

    #MQTT callbacks
    def mqtt_on_connect(self, client, flags, rc):
        app.logger.debug("Connected with result code %s", rc)
        if rc == 0:
            self.mqtt.con = self.open_db()
            client.subscribe("/home/meterDigitizer/+/value")

    def mqtt_on_disconnect(self, client, rc):
        if self.mqtt.con:
            self.mqtt.con.close()
        app.logger.debug("Disconnected with result code %s", rc)


    def mqtt_on_message(self, client, msg):
        jsonMsg = json.loads(msg.payload.decode())
        with self.mqtt.con:
            if jsonMsg['id'] not in self.mqtt.knownSensors:
                self.mqtt.con.execute('INSERT OR REPLACE INTO "sensors" VALUES (:id, :name)', jsonMsg)
                self.mqtt.knownSensors.append(jsonMsg['id'])
            self.mqtt.con.execute('INSERT OR IGNORE INTO "values" VALUES (datetime(:timestamp, "localtime"), :id, :value)',
                jsonMsg)
    #bind to PORT
    def mqtt_connect(self):
        self.mqtt.client.user_data_set(self)
        self.mqtt.client.on_message = mqtt_on_message
        self.mqtt.client.on_connect = mqtt_on_connect
        self.mqtt.client.on_disconnect = mqtt_on_disconnect
        connected = False
        if "port" not in config["MQTT"]:
            try:
                connected = self.mqtt.client.connect_srv(config["MQTT"]["Server"], keepalive = config["MQTT"]["keepalive"])
            except ValueError:
                connected = False
        if not connected:
            self.mqtt.client.connect_async(config["MQTT"]["Server"], port=config.get("MQTT", "Port", fallback=1883))

    #Database operations
    def open_db(self):
        dbName = os.path.abspath(config["DB"]["database"])
        dbPath = os.path.dirname(dbName)
        if not os.path.exists(dbPath):
            print("Creating path %s"%dbPath)
            os.makedirs(dbPath, mode=0o770, exist_ok=True)
        con = sqlite3.connect(database=config["DB"]["database"]
            , detect_types=sqlite3.PARSE_DECLTYPES)
        con.row_factory = sqlite3.Row
        return con

    def create_db(self, con):
        with con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS "sensors" (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                    )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS "values" (
                    "timestamp" TIMESTAMP NOT NULL,
                    "id" INTEGER NOT NULL,
                    "value" REAL NOT NULL,
                    FOREIGN KEY(id) REFERENCES sensors(id),
                    PRIMARY KEY(id, timestamp)
                    )
            """)
        return con

    def __init__(self):
        self.create_db(self.open_db()).close()
        self.mqtt_connect()
        self.mqtt.client.loop_start()
        app.logger.info("Started!")

    def __del__(self, *err):
        self.mqtt.client.disconnect()
        self.mqtt.client.loop_stop()
        self.web_db_connection.close()
        app.logger.info("Stopped!")

    def index(self):
        with self.web_db_connection:
            rows = self.web_db_connection.execute('SELECT * FROM "sensors" ORDER BY "id"')
            return render_template("index.html.j2",
                title="Meter Digitizer",
                sensors=rows
            )

    def show_sensor(self, sensor_id):
        with self.web_db_connection as con:
            sensor_name = con.execute('''
                    SELECT name FROM sensors WHERE "id"=?
                ''', (sensor_id,)).fetchone()["name"]
            return render_template("sensor.html.j2",
                title = "Meter Digitizer: %s"%sensor_name,
                id = sensor_id
                )
    def show_sensor_values(self, sensor_id):
        with self.web_db_connection as con:
            cursor = con.execute('''
                SELECT "timestamp", "value"
                    FROM "values"
                    WHERE "values"."id"=?
                    ORDER BY "timestamp"
                ''', (sensor_id,))
            data = differentiate_data(cursor);
            return json.jsonify(data);


app = Flask(__name__)
md = None
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
    md = MeterDigitizer()

@app.before_first_request
def open_database(*args, **kwargs):
    md.web_db_connection = md.open_db()

@app.route('/')
def index():
    return md.index()

@app.route('/sensor/<int:sensor_id>')
def show_sensor(sensor_id):
    return md.show_sensor(sensor_id)

@app.route('/sensor/<int:sensor_id>.json')
def show_sensor_values(sensor_id):
    return md.show_sensor_values(sensor_id)

class JSONEncoder(json.JSONEncoder):
    """docstring for JSONEncoder."""
    def default(self, o):
        if isinstance(o, datetime.date):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

app.json_encoder = JSONEncoder

if __name__ == '__main__':
    configport = config.get('Server','port', fallback="8080")
    port = int(os.environ.get('PORT', configport))
    try:
        app.run(host="0.0.0.0", port=port)
    except KeyboardInterrupt:
        pass
