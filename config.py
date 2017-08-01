def _createConfig():
    import configargparse
    import os.path
    default_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "config.txt")

    config = configargparse.getArgumentParser(
        default_config_files=[default_config_file],
        auto_env_var_prefix="MD_",
        args_for_setting_config_path = ["--config", "-c"]
    )
    http = config.add_argument_group("HTTP server options")
    http.add_argument("--fcgi", action="store_true",
                      help="FastCGI mode")
    http.add_argument("--http-bind", default="127.0.0.1",
                      help="bind address")
    http.add_argument("--http-port", type=int, default=8080,
                      help="listening port")
    mqtt_broker = config.add_argument_group("MQTT broker options")
    mqtt_broker.add_argument("--mqtt-server", default="localhost",
                             help="MQTT broker server address")
    mqtt_broker.add_argument("--mqtt-port",
                             help="MQTT broker port")
    mqtt_broker.add_argument("--mqtt-keep-alive", type=int, default=60,
                             help="MQTT client keep alive timeout in seconds")
    mqtt_topic = config.add_argument_group("MQTT message topic")
    mqtt_topic.add_argument("--mqtt-topic-sensor", default="/home/meterDigitizer/+/value",
                            help="MQTT Topic template for sensor values")
    db = config.add_argument_group("Database");
    db.add_argument("--db", default="db/meterDigitizer.sqlite3",
                    help="Database file path")
    return config

config = _createConfig()
if __name__ == '__main__':
    args = config.parse_args();
    for key,val in sorted(vars(args).items()):
        print(key,"=",val)

args = None
def parse_args(*posargs, **kwargs):
    global args
    args = config.parse_args(*posargs, **kwargs)
    return args
