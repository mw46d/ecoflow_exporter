import datetime
import logging as log
import sys
import os
import signal
import ssl
import time
import json
import re
import base64
import uuid
from queue import Queue
from threading import Timer
from multiprocessing import Process
import requests
import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, REGISTRY, Gauge, Counter


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


class EcoflowMetricException(Exception):
    pass


class EcoflowAuthentication:
    def __init__(self, ecoflow_username, ecoflow_password, ecoflow_api_host):
        self.ecoflow_username = ecoflow_username
        self.ecoflow_password = ecoflow_password
        self.ecoflow_api_host = ecoflow_api_host
        self.mqtt_url = "mqtt.ecoflow.com"
        self.mqtt_port = 8883
        self.mqtt_username = None
        self.mqtt_password = None
        self.mqtt_client_id = None
        self.authorize()

    def authorize(self):
        url = f"https://{self.ecoflow_api_host}/auth/login"
        headers = {"lang": "en_US", "content-type": "application/json"}
        data = {"email": self.ecoflow_username,
                "password": base64.b64encode(self.ecoflow_password.encode()).decode(),
                "scene": "IOT_APP",
                "userType": "ECOFLOW"}

        log.info(f"Login to EcoFlow API {url}")
        request = requests.post(url, json=data, headers=headers)
        response = self.get_json_response(request)

        try:
            token = response["data"]["token"]
            user_id = response["data"]["user"]["userId"]
            user_name = response["data"]["user"]["name"]
        except KeyError as key:
            raise Exception(f"Failed to extract key {key} from response: {response}")

        log.info(f"Successfully logged in: {user_name}")

        url = f"https://{self.ecoflow_api_host}/iot-auth/app/certification"
        headers = {"lang": "en_US", "authorization": f"Bearer {token}"}
        data = {"userId": user_id}

        log.info(f"Requesting IoT MQTT credentials {url}")
        request = requests.get(url, data=data, headers=headers)
        response = self.get_json_response(request)

        try:
            self.mqtt_url = response["data"]["url"]
            self.mqtt_port = int(response["data"]["port"])
            self.mqtt_username = response["data"]["certificateAccount"]
            self.mqtt_password = response["data"]["certificatePassword"]
            self.mqtt_client_id = f"ANDROID_{str(uuid.uuid4()).upper()}_{user_id}"
            self.mqtt_user_id = user_id
        except KeyError as key:
            raise Exception(f"Failed to extract key {key} from {response}")

        log.info(f"Successfully extracted account: {self.mqtt_username}")

    @staticmethod
    def get_json_response(request):
        if request.status_code != 200:
            raise Exception(f"Got HTTP status code {request.status_code}: {request.text}")

        try:
            response = json.loads(request.text)
            response_message = response["message"]
        except KeyError as key:
            raise Exception(f"Failed to extract key {key} from {request.text}")
        except Exception as error:
            raise Exception(f"Failed to parse response: {request.text} Error: {error}")

        if response_message.lower() != "success":
            raise Exception(f"{response_message}")

        return response


class EcoflowDevice():

    def __init__(self, device_sn, device_name, device_type):
        self.device = None
        self.device_sn = device_sn
        self.device_name = device_name
        self.device_type = device_type
        self.topic = f"/app/device/property/{device_sn}"
        self.last_message_time = None
        self.handled_messages = 0

class EcoflowMQTT():

    def __init__(self, auth, user_data, message_queue, timeout_seconds):
        self.addr = auth.mqtt_url
        self.port = auth.mqtt_port
        self.username = auth.mqtt_username
        self.password = auth.mqtt_password
        self.client_id = auth.mqtt_client_id
        self.user_id = auth.mqtt_user_id
        self.user_data = user_data
        self.timeout_seconds = timeout_seconds
        self.client = None
        self.idle_timer = None
        self.ping_timer = None
        self.message_queue = message_queue
        self.last_message_time = None


    @staticmethod
    def __is_json(payload: bytes) -> bool:
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            return False

        text = text.strip()
        if not (text.startswith("{") or text.startswith("[")):
            return False

        try:
            json.loads(text)
            return True
        except json.JSONDecodeError:
            return False


    def connect(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()

        if self.idle_timer != None:
            self.idle_timer.cancel();
            self.idle_timer = None

        self.client = mqtt.Client(
            client_id = self.client_id,
            callback_api_version = mqtt.CallbackAPIVersion.VERSION2,
            userdata = self.user_data
        )
        self.client.username_pw_set(self.username, self.password)
        self.client.tls_set(certfile = None, keyfile = None, cert_reqs = ssl.CERT_REQUIRED)
        self.client.tls_insecure_set(False)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        log.info(f"Connecting to MQTT Broker {self.addr}:{self.port} using client id {self.client_id}")
        self.client.connect(self.addr, self.port)
        self.client.loop_start()

        self.idle_timer = RepeatTimer(10, self.idle_reconnect)
        self.idle_timer.daemon = True
        self.idle_timer.start()

    def idle_reconnect(self):
        if self.last_message_time and time.time() - self.last_message_time > self.timeout_seconds:
            log.error(f"No messages received for {self.timeout_seconds} seconds. Reconnecting to MQTT")
            # We pull the following into a separate process because there are actually quite a few things that can go
            # wrong inside the connection code, including it just timing out and never returning. So this gives us a
            # measure of safety around reconnection
            while True:
                connect_process = Process(target=self.connect)
                connect_process.start()
                connect_process.join(timeout=60)
                connect_process.terminate()
                if connect_process.exitcode == 0:
                    log.info("Reconnection successful, continuing")
                    # Reset last_message_time here to avoid a race condition between idle_reconnect getting called again
                    # before on_connect() or on_message() are called
                    self.last_message_time = None
                    break
                else:
                    log.error("Reconnection errored out, or timed out, attempted to reconnect...")


    def request_latest_quotas_device(self, ef_d):
        message  = {
            "id":          str(round(time.time() * 1000)),
            "version":     "1.1",
            "from":        "Android",
            "operateType": "latestQuotas",
            "params":      {},
        }

        payload = json.dumps(message)

        topic = f"/app/{self.user_id}/{ef_d.device_sn}/thing/property/get"

        info = self.client.publish(topic, payload, 1)
        info.wait_for_publish(5.0)

        if info.is_published:
            log.info(f"Published to {topic}")
            t = time.time()
            if self.last_message_time == None or t > self.last_message_time:
                self.last_message_time = time.time()
            ef_d.last_message_time = self.last_message_time
        else:
            log.info(f"Failed to publish to {topic}")


    def request_latest_quotas(self):
        for d in self.user_data:
            self.request_latest_quotas_device(d)


    def ping_device(self, ef_d):
        now = datetime.datetime.now(datetime.UTC)

        message = {
            "id":          str(round(time.time() * 1000)),
            "version":     "1.0",
            "from":        "Android",
            "operateType": "setRtcTime",
            "moduleType":  2,
            "params": {
                "min":   now.minute,
                "day":   now.day,
                "week":  now.isoweekday(),
                "sec":   now.second,
                "month": now.month,
                "hour":  now.hour,
                "year":  now.year,
            },
        }

        payload = json.dumps(message)

        topic = f"/app/{self.user_id}/{ef_d.device_sn}/thing/property/set"
        info = self.client.publish(topic, payload, 1)
        info.wait_for_publish(5.0)

        if info.is_published:
            log.info(f"Published to {topic}")
        else:
            log.info(f"Failed to publish to {topic}")


    def ping(self):
        for d in self.user_data:
            self.ping_device(d)


    def on_connect(self, client, _userdata, _flags, reason_code, _properties):
        # Initialize the time of last message at least once upon connection so that other things that rely on that to be
        # set (like idle_reconnect) work
        match reason_code:
            case "Success":
                self.request_latest_quotas()
                for d in self.user_data:
                    self.client.subscribe(d.topic)
                    log.info(f"Subscribed to MQTT topic {d.topic}")
                if self.ping_timer == None:
                    self.ping_timer = RepeatTimer(45, self.ping)
                    self.ping_timer.daemon = True
                    self.ping_timer.start()
                    log.info(f"Started RTC ping timer")
                else:
                    log.warning(f"RTC ping timer still exists??")
            case "Keep alive timeout":
                log.error("Failed to connect to MQTT: connection timed out")
            case "Unsupported protocol version":
                log.error("Failed to connect to MQTT: unsupported protocol version")
            case "Client identifier not valid":
                log.error("Failed to connect to MQTT: invalid client identifier")
            case "Server unavailable":
                log.error("Failed to connect to MQTT: server unavailable")
            case "Bad user name or password":
                log.error("Failed to connect to MQTT: bad username or password")
            case "Not authorized":
                log.error("Failed to connect to MQTT: not authorised")
            case _:
                log.error(f"Failed to connect to MQTT: another error occurred: {reason_code}")

        return client


    # XXX ? @staticmethod
    def on_disconnect(self, _client, _userdata, _flags, reason_code, _properties): # noqa: F841
        if reason_code > 0:
            log.error(f"Unexpected MQTT disconnection: {reason_code}. Will auto-reconnect")
            if self.ping_timer != None:
                self.ping_timer.cancel();
                self.ping_timer = None
                log.info(f"RTC ping timer canceled")
            time.sleep(5)


    def on_message(self, _client, userdata, message):
        ef_d = None
        for d in userdata:
            if d.topic == message.topic:
                ef_d = d

        if ef_d == None:
            log.error(f"No device found for {message.topic}")
            return

        if ef_d.device_type is None:
            self.message_queue.put({ 'ef_device': ef_d, 'message': message.payload.decode("utf-8") })
        else:
            if ef_d.device is None:
                match ef_d.device_type:
                    case 'android':
                        from devices.android import Android
                        ef_d.device = Android()
                    case 'river3':
                        from devices.river3 import EcoflowRiver3
                        ef_d.device = EcoflowRiver3()
                    case 'unsupported':
                        return
                    case _:
                        ef_d.device_type = 'unsupported'
                        log.error(f"Unsupported device: {ef_d.device_type}")

            if ef_d.device is not None:
                self.message_queue.put({ 'ef_device': ef_d, 'message': ef_d.device.get_payload(raw_data = message.payload) })

        t = time.time()
        if self.last_message_time == None or t > self.last_message_time:
            self.last_message_time = time.time()

        ef_d.last_message_time = t


class EcoflowMetric:
    def __init__(self, metric_data: dict):
        self.ecoflow_payload_key = metric_data['name']
        self.name = f"ecoflow_{self.convert_ecoflow_key_to_prometheus_name()}"
        self.metric = Gauge(self.name, f"value from MQTT object key {self.ecoflow_payload_key}", labelnames = metric_data['labels'].keys())


    def convert_ecoflow_key_to_prometheus_name(self):
        # bms_bmsStatus.maxCellTemp -> bms_bms_status_max_cell_temp
        # pd.ext4p8Port -> pd_ext4p8_port
        key = self.ecoflow_payload_key.replace('.', '_')
        new = key[0].lower()
        for character in key[1:]:
            if character.isupper() and not new[-1] == '_':
                new += '_'
            new += character.lower()
        # Check that metric name complies with the data model for valid characters
        # https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
        if not re.match("[a-zA-Z_:][a-zA-Z0-9_:]*", new):
            raise EcoflowMetricException(f"Cannot convert payload key {self.ecoflow_payload_key} to comply with the Prometheus data model. Please, raise an issue!")
        return new


    def set(self, labels, value):
        # According to best practices for naming metrics and labels, the voltage should be in volts and the current in amperes
        # WARNING! This will ruin all Prometheus historical data and backward compatibility of Grafana dashboard
        # value = value / 1000 if value.endswith("_vol") or value.endswith("_amp") else value
        log.debug(f"Set {self.name} = {value}")
        self.metric.labels(**labels).set(value)


    def clear(self, labels):
        log.debug(f"Clear {self.name}")
        self.metric.labels(**labels).clear()


class Worker:
    def __init__(self, message_queue, device_list, collecting_interval_seconds = 10):
        self.message_queue = message_queue
        self.device_list = device_list
        self.collecting_interval_seconds = collecting_interval_seconds
        self.metrics_collector = []
        self.online = Gauge("ecoflow_online", "1 if device is online", labelnames=["device"])
        self.mqtt_messages_receive_total = Counter("ecoflow_mqtt_messages_receive_total", "total MQTT messages", labelnames=["device"])


    def loop(self):
        time.sleep(self.collecting_interval_seconds)
        while True:
            queue_size = self.message_queue.qsize()
            if queue_size > 0:
                log.info(f"Processing {queue_size} event(s) from the message queue")
            # if queue_size > 0:
            #     log.info(f"Processing {queue_size} event(s) from the message queue")
            #     self.online.labels(device=self.device_name).set(1)
            #     self.mqtt_messages_receive_total.labels(device=self.device_name).inc(queue_size)
            # else:
            #     log.info("Message queue is empty. Assuming that the device is offline")
            #     self.online.labels(device=self.device_name).set(0)
            #     # Clear metrics for NaN (No data) instead of last value
            #     for metric in self.metrics_collector:
            #         metric.clear()

            handled_devices = []
            while not self.message_queue.empty():
                m = self.message_queue.get()
                ef_d = m['ef_device']
                payload = m['message']
                log.debug(f"Received payload: {payload} for {ef_d.device_sn}")
                if payload is None:
                    continue

                try:
                    payload = json.loads(payload)
                    params = payload['params']
                    self.process_payload(ef_d.device_name, params)
                    handled_devices.append(ef_d)
                    ef_d.handled_messages += 1
                except KeyError as key:
                    log.error(f"Failed to extract key {key} from payload: {payload}")
                except Exception as error:
                    log.error(f"Failed to parse MQTT payload: {payload} Error: {error}")
                    continue

            for d in self.device_list:
                seen = False
                for hd in handled_devices:
                    if d == hd:
                        self.mqtt_messages_receive_total.labels(device = hd.device_name).inc(hd.handled_messages)
                        hd.handled_messages = 0
                        self.online.labels(device = hd.device_name).set(1)
                        seen = True
                        break

                if not seen:
                    if d.last_message_time and time.time() - d.last_message_time > 60:
                        # I want at least one message per minute
                        self.online.labels(device = d.device_name).set(0)
                        # XXX How to clear old values?
                    d.handled_messages = 0

            time.sleep(self.collecting_interval_seconds)


    def get_metric_by_ecoflow_payload_key(self, ecoflow_payload_key):
        for metric in self.metrics_collector:
            if metric.ecoflow_payload_key == ecoflow_payload_key:
                log.debug(f"Found metric {metric.name} linked to {ecoflow_payload_key}")
                return metric
        log.debug(f"Cannot find metric linked to {ecoflow_payload_key}")
        return False


    def process_payload(self, device_name, params):
        log.debug(f"Processing params: {device_name}:{params}")
        # messages for multiple batteries connected to the same Ocean Pro inverter
        # have different product_sn's
        product_sn = params.pop('product_sn', None)

        labels = { 'device': device_name }
        if product_sn:
            labels['product_sn'] = product_sn

        for ecoflow_payload_key in params.keys():
            ecoflow_payload_value = params[ecoflow_payload_key]
            if not isinstance(ecoflow_payload_value, (int, float, str, list)):
                log.warning(f"Skipping unsupported metric(1) {type(ecoflow_payload_value)} = {ecoflow_payload_key}: {ecoflow_payload_value}")
                continue

            metrics = []
            if isinstance(ecoflow_payload_value, (int, float)):
                metrics.append({
                    'name': ecoflow_payload_key,
                    'value': ecoflow_payload_value,
                    'labels': labels
                })

            if isinstance(ecoflow_payload_value, str):
                if len(ecoflow_payload_value) == 0:
                    log.warning(f"Skipping empty string value {type(ecoflow_payload_value)} = {ecoflow_payload_key}: {ecoflow_payload_value}")
                    continue
                metrics.append({
                    'name': ecoflow_payload_key,
                    'value': 0,
                    'labels': {
                        **labels,
                        'value': ecoflow_payload_value
                    }
                })

            if isinstance(ecoflow_payload_value, list):
                for index, value in enumerate(ecoflow_payload_value):
                    if not isinstance(value, (int, float, dict)):
                        log.warning(f"Skipping unsupported metric(3) {type(value)} = {ecoflow_payload_key}: {ecoflow_payload_value}->[{index}]{value}")
                        continue

                    if isinstance(value, (int, float)):
                        metrics.append({
                            'name': ecoflow_payload_key,
                            'value': value,
                            'labels': {
                                **labels,
                                'num': index,
                            }
                        })

                    if isinstance(value, dict) and 'statistics_object' in value:
                        metric_key =  value['statistics_object'].removeprefix("STATISTICS_OBJECT_").lower()
                        metric_value = 0
                        if 'statistics_content' in  value:
                            metric_value = value['statistics_content']

                        metrics.append({
                            'name': f"statistics_{metric_key}",
                            'value': metric_value,
                            'labels': labels
                        })

            for _metric in metrics:
                # metric = self.get_metric_by_ecoflow_payload_key(_metric)
                metric = self.get_metric_by_ecoflow_payload_key(_metric['name'])
                if not metric:
                    try:
                        metric = EcoflowMetric(_metric)

                    except EcoflowMetricException as error:
                        log.error(error)
                        continue

                    log.info(f"Created new metric from payload key {metric.ecoflow_payload_key} -> {metric.name}")
                    self.metrics_collector.append(metric)

                metric.metric.labels(**_metric['labels']).set(_metric['value'])

                if ecoflow_payload_key == 'inv.acInVol' and ecoflow_payload_value == 0:
                    ac_in_current = self.get_metric_by_ecoflow_payload_key('inv.acInAmp')
                    if ac_in_current:
                        log.debug("Set AC inverter input current to zero because of zero inverter voltage")
                        ac_in_current.metric.labels(**_metric['labels']).set(0)


def signal_handler(signum, _frame):
    log.info(f"Received signal {signum}. Exiting...")
    sys.exit(0)


def main():
    # Register the signal handler for SIGTERM
    signal.signal(signal.SIGTERM, signal_handler)

    # Disable Process and Platform collectors
    # pylint: disable=protected-access
    for coll in list(REGISTRY._collector_to_names.keys()):
        REGISTRY.unregister(coll)

    log_level = os.getenv("LOG_LEVEL", "WARNING")

    match log_level:
        case "DEBUG":
            log_level = log.DEBUG
        case "INFO":
            log_level = log.INFO
        case "WARNING":
            log_level = log.WARNING
        case "ERROR":
            log_level = log.ERROR
        case _:
            log_level = log.WARNING

    log.basicConfig(stream=sys.stdout, level=log_level, format='%(asctime)s %(levelname)-7s %(message)s')

    device_pretty_names_j = os.getenv("DEVICES_PRETTY_NAMES")
    device_sn = os.getenv("DEVICE_SN")
    device_name = os.getenv("DEVICE_NAME") or device_sn
    device_type = os.getenv("DEVICE_TYPE") or None
    ecoflow_username = os.getenv("ECOFLOW_USERNAME")
    ecoflow_password = os.getenv("ECOFLOW_PASSWORD")
    ecoflow_api_host = os.getenv("ECOFLOW_API_HOST", "api.ecoflow.com")
    exporter_port = int(os.getenv("EXPORTER_PORT", "9090"))
    collecting_interval_seconds = int(os.getenv("COLLECTING_INTERVAL", "10"))
    timeout_seconds = int(os.getenv("MQTT_TIMEOUT", "60"))

    if not device_pretty_names_j and not device_sn or not ecoflow_username or not ecoflow_password:
        log.error("Please, provide all required environment variables: DEVICE_SN, ECOFLOW_USERNAME, ECOFLOW_PASSWORD")
        sys.exit(1)

    try:
        auth = EcoflowAuthentication(ecoflow_username, ecoflow_password, ecoflow_api_host)
    except Exception as error:
        log.error(error)
        sys.exit(1)

    device_map = {}
    if device_pretty_names_j:
        try:
            device_map = json.loads(device_pretty_names_j)
        except Exception as e:
            log.error("DEVICES_PRETTY_NAMES was not valid JSON, make sure it has format {\"R33XXXXXXXXX\":\"My Delta 2\", \"R33YYYYY\":\"Delta Pro backup\"}. Original error: {e}")

    if device_sn:
        device_a = device_sn.split(',')

        for d in device_a:
            if d not in device_map:
                device_map[d] = d

    if len(device_map) == 0:
        log.error("No devices found")
        sys.exit(1)

    log.info(f"devices: {device_map}")

    ef_devices = []
    message_queue = Queue()
    ef_client = EcoflowMQTT(auth, ef_devices, message_queue, timeout_seconds)

    for d in device_map:
        d_type = None
        if d.startswith('HR51ZA1AVH') or d.startswith('HR61ZA1AVH'):
            d_type = 'android'
        d_name = device_map[d]
        if len(d_name) == 0:
            d_name = d

        ef_devices.append(EcoflowDevice(d, d_name, d_type))

    metrics_worker = Worker(message_queue, ef_devices, collecting_interval_seconds)

    log.info(f"Starting http server on {exporter_port}")
    start_http_server(exporter_port)

    try:
        ef_client.connect();
        metrics_worker.loop()

    except KeyboardInterrupt:
        log.info("Received KeyboardInterrupt. Exiting...")
        sys.exit(0)


if __name__ == '__main__':
    main()
