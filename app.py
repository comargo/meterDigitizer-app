#!/usr/bin/env python3
import config

import os, sys, datetime
import sqlite3
from flask import Flask, render_template, request, json
from diffirentiate_data import differentiate_data

if __name__ == '__main__':
    sys.exit("Use launch.py to start")

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.date):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

class MeterDigitizer(Flask):
    def __init__(self, import_name=__name__,*args, **kwargs):
        kwargs['import_name'] = import_name
        Flask.__init__(self, *args, **kwargs)
        self.json_encoder = JSONEncoder
        self.mqtt_client = None
        self.web_db = None
        if self.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
            return;
        self.create_db(self.open_db()).close()
        self.create_routes()
        self.mqtt_connect()

    def __del__(self, *err):
        if self.mqtt_client:
            self.mqtt_client.disconnect()
            self.mqtt_client.loop_stop()
        if self.web_db:
            self.web_db.close()

    #MQTT callbacks

    #bind to PORT
    def mqtt_connect(self):
        import paho.mqtt.client as mqtt
        def mqtt_on_connect(client, userdata, flags, rc):
            self.logger.debug("Connected with result code %s", rc)
            if rc == 0:
                client.md_db = self.open_db()
                client.subscribe(config.args.mqtt_topic_sensor)

        def mqtt_on_disconnect(client, userdata, rc):
            if client.md_db:
                client.md_db.close()
            self.logger.debug("Disconnected with result code %s", rc)

        def mqtt_on_message(client, userdata, msg):
            jsonMsg = json.loads(msg.payload.decode())
            with client.md_db:
                if jsonMsg['id'] not in client.knownSensors:
                    client.md_db.execute('INSERT OR REPLACE INTO "sensors" VALUES (:id, :name)', jsonMsg)
                    client.knownSensors.append(jsonMsg['id'])
                client.md_db.execute('INSERT OR IGNORE INTO "values" VALUES (datetime(:timestamp, "localtime"), :id, :value)',
                    jsonMsg)

        self.mqtt_client = mqtt.Client(userdata=self)
        self.mqtt_client.md_database = None
        self.mqtt_client.knownSensors = []
        self.mqtt_client.on_message = mqtt_on_message
        self.mqtt_client.on_connect = mqtt_on_connect
        self.mqtt_client.on_disconnect = mqtt_on_disconnect
        connected = False
        if not config.args.mqtt_port:
            try:
                connected = self.mqtt_client.connect_srv(config.args.mqtt_server, keepalive = config.args.mqtt_keep_alive)
            except ValueError:
                connected = False
        if not connected:
            self.mqtt_client.connect_async(config.args.mqtt_server, config.args.mqtt_port or 1883)
        self.mqtt_client.loop_start()

    def create_routes(self):
        @self.before_first_request
        def open_database():
            self.web_db = self.open_db()

        @self.route('/')
        def index():
            rows = self.web_db.execute('SELECT * FROM "sensors" ORDER BY "id"')
            return render_template("index.html.j2",
                title="Meter Digitizer",
                sensors=rows
            )

        @self.route('/sensor/<int:sensor_id>')
        def show_sensor(sensor_id):
            sensor_name = self.web_db.execute('''SELECT name FROM sensors WHERE "id"=?''',
                                            (sensor_id,)).fetchone()["name"]
            return render_template("sensor.html.j2",
                                   title = "Meter Digitizer: %s"%sensor_name,
                                   id = sensor_id
                                   )

        @self.route('/sensor/<int:sensor_id>.json')
        def show_sensor_values(sensor_id):
            cursor = self.web_db.execute('''
                SELECT "timestamp", "value"
                    FROM "values"
                    WHERE "values"."id"=?
                    ORDER BY "timestamp"
                ''', (sensor_id,))
            data = differentiate_data(cursor);
            return json.jsonify(data);

    #Database operations
    def open_db(self):
        dbName = os.path.abspath(config.args.db)
        dbPath = os.path.dirname(dbName)
        if not os.path.exists(dbPath):
            print("Creating path %s"%dbPath)
            os.makedirs(dbPath, mode=0o770, exist_ok=True)
        con = sqlite3.connect(database=dbName
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
