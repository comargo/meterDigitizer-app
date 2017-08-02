import random
from datetime import datetime, timedelta
from math import ceil

def _sensorSeries(sensorID, sensorName, startTime,
                  timespan, length, offset):
    curTime = startTime
    second = timedelta(seconds = 1)
    series = []
    value = 0

    while curTime < startTime+timespan:
        randOffset = random.randrange(offset.total_seconds())
        randLength = random.randrange(length.total_seconds())
        flowSpeed = random.random()
        quantity = ceil(randLength*flowSpeed)
        curTime += second*randOffset
        for _ in range(quantity):
            curTime += second/flowSpeed;
            value += 1
            yield {
                "timestamp": curTime.replace(microsecond=0),
                "id": sensorID,
                "name": sensorName,
                "value": "%.3f"%(value/1000.0)
            }

def generate(sensors,
             timespan,
             length,
             offset):
    startTime = datetime.now().replace(microsecond=0)
    for i in range(sensors):
        for value in _sensorSeries(i, "sensor%s"%i, startTime, timespan, length, offset):
            yield value
