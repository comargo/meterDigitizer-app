#!/usr/bin/env python3
import config
import json
import paho.mqtt.client

def mqtt_on_connect(client, userdata, flags, rc):
    logger.debug("Connected with result code %s", rc)
    if rc == 0:
        userdata["db"] = open_db()
        client.subscribe(config.args.mqtt_topic_sensor)

def mqtt_on_disconnect(client, userdata, rc):
    if userdata.db:
        userdata["db"].close()
    logger.debug("Disconnected with result code %s", rc)

def mqtt_on_message(client, userdata, msg):
    jsonMsg = json.loads(msg.payload.decode())
    with userdata["db"]:
        if jsonMsg['id'] not in userdata["known_sensors"]:
            userdata["db"].execute('INSERT OR REPLACE INTO "sensors" VALUES (:id, :name)', jsonMsg)
            userdata["known_sensors"].append(jsonMsg['id'])
        userdata["db"].execute('INSERT OR IGNORE INTO "values" VALUES (datetime(:timestamp, "localtime"), :id, :value)',
            jsonMsg)

def main(mqtt_class):
    mqtt_data = {
        "db": None,
        "known_sensors": []
    }
    config.parse_args();
    mqtt_client = mqtt_class(userdata=mqtt_data)
    mqtt_client.on_message = mqtt_on_message
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_disconnect = mqtt_on_disconnect
    connected = False
    if not config.args.mqtt_port:
        try:
            connected = self.mqtt_client.connect_srv(config.args.mqtt_server, keepalive = config.args.mqtt_keep_alive)
        except ValueError:
            connected = False
    if not connected:
        self.mqtt_client.connect_async(config.args.mqtt_server, config.args.mqtt_port or 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    main(paho.mqtt.client.Client)
