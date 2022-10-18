import calendar
import sys
from datetime import datetime

import paho.mqtt.client as mqtt
import requests
import snappy

from src.proto.prometheus_pb2 import (
    WriteRequest
)


class PrometheusRemoteWrite(object):
    def __init__(self, url: str):
        self.url = url

    def dt2ts(self, dt: datetime):
        """Converts a datetime object to UTC timestamp
        naive datetime will be considered UTC.
        """
        return calendar.timegm(dt.utctimetuple())

    def write(self, name: str, labels: dict, value: int or float, timestamp: datetime = None):
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

    def __init__(self, subscribe_prefix: str, prometheus_remote_write_url: str):
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
            print('Skip this message. Not have metric_name. [{msg.topic}]')
            return

        metric_name = split_topic[1]
        labels = dict(zip(split_topic[2::2], split_topic[3::2]))

        print(metric_name, labels, msg.payload)
        self.remote_write.write(metric_name, labels, float(msg.payload))


def main():
    url = "http://localhost:10908/api/v1/receive"
    prefix = "metrics"

    try:
        client = SubscribeMetricsClient(prefix, url)
        client.connect("localhost", 1883, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == '__main__':
    main()
