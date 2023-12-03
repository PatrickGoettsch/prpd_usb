import json
import time

from paho.mqtt import publish

from .. import prpd


def main(prpd_reader, args):
    auth = {}
    print(args)
    if args.mqtt_password and args.mqtt_username:
        auth["password"] = args.mqtt_password
        auth["username"] = args.mqtt_username
    while True:
        messages = []
        for command, field, _time, value in prpd_reader.read():
            if args.mqtt_payload_simple:
                payload = value
            else:
                payload = {
                    "value": value,
                    "unit": field.unit,
                }
            messages.append({
                "topic": f"{args.mqtt_prefix}/{command.name}/{field.name}",
                "payload": json.dumps(payload),
            })
        publish.multiple(messages, hostname=args.mqtt_hostname, port=args.mqtt_port, auth=auth)
        time.sleep(args.mqtt_interval)
