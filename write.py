from datetime import datetime
from prometheus_pb2 import (
    TimeSeries,
    Label,
    Labels,
    Sample,
    WriteRequest
)
import calendar
import logging
import requests
import snappy

def dt2ts(dt):
    """Converts a datetime object to UTC timestamp
    naive datetime will be considered UTC.
    """
    return calendar.timegm(dt.utctimetuple())

def write():
    write_request = WriteRequest()

    series = write_request.timeseries.add()

    # name label always required
    label = series.labels.add()
    label.name = "__name__"
    label.value = "metric_name"
    
    # as many labels you like
    label = series.labels.add()
    label.name = "ssl_cipher"
    label.value = "some_value"

    sample = series.samples.add()
    sample.value = 42 # your count?
    sample.timestamp = dt2ts(datetime.utcnow()) * 1000

    uncompressed = write_request.SerializeToString()
    compressed = snappy.compress(uncompressed)

    url = "http://localhost:7201/api/v1/prom/remote/write"
    headers = {
        "Content-Encoding": "snappy",
        "Content-Type": "application/x-protobuf",
        "X-Prometheus-Remote-Write-Version": "0.1.0",
        "User-Agent": "metrics-worker"
    }
    try:
        response = requests.post(url, headers=headers, data=compressed)
    except exception as e:
        print(e)

write()
