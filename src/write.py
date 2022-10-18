import calendar
from datetime import datetime

import paho.mqtt.client as mqtt
import requests
import snappy

from src.proto.prometheus_pb2 import (
    WriteRequest
)


class PrometheusRemoteWrite(object):
    def __init__(self, url):
        self.url = url

    def dt2ts(self, dt):
        """Converts a datetime object to UTC timestamp
        naive datetime will be considered UTC.
        """
        return calendar.timegm(dt.utctimetuple())

    def write(self, name, labels, value, timestamp=None):
        if not timestamp:
            timestamp = datetime.utcnow()

        write_request = WriteRequest()

        series = write_request.timeseries.add()

        # name label always required
        label = series.labels.add()
        label.name = "__name__"
        label.value = name

        for k, v in labels.items():
            label = series.labels.add()
            label.name = k
            label.value = v

        sample = series.samples.add()
        sample.value = value
        sample.timestamp = self.dt2ts(timestamp) * 1000

        uncompressed = write_request.SerializeToString()
        compressed = snappy.compress(uncompressed)

        headers = {
            "Content-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "X-Prometheus-Remote-Write-Version": "0.1.0",
            "User-Agent": "metrics-worker"
        }

        return requests.post(self.url, headers=headers, data=compressed)


class SubscribeMetricsClient(mqtt.Client):

    def __init__(self, subscribe_prefix, prometheus_remote_write_url):
        self.prometheus_remote_write_url = prometheus_remote_write_url
        self.subscribe_prefix = subscribe_prefix
        self.remote_write = PrometheusRemoteWrite(self.prometheus_remote_write_url)
        super().__init__()

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))
        client.subscribe(f'{self.subscribe_prefix}/#')

    def on_message(self, client, userdata, msg):
        print(msg.topic + " " + str(msg.payload))
        split_topic = msg.topic.split('/')
        labels = {}

        if len(split_topic) < 1:
            print('skip message. not have name. [{msg.topic}]')
            return
        if len(split_topic) > 2:
            labels = dict(zip(split_topic[2::2], split_topic[3::2]))

        metric_name = split_topic[1]
        print(metric_name, labels, msg.payload)
        self.remote_write.write(metric_name, labels, float(msg.payload))


if __name__ == '__main__':
    url = "http://localhost:10908/api/v1/receive"
    prefix = "metrics"

    client = SubscribeMetricsClient(prefix, url)
    client.connect("localhost", 1883, 60)
    client.loop_forever()
