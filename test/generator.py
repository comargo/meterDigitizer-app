import random
from datetime import datetime, timedelta
from math import ceil

def _sensorSeries(sensorID, sensorName, startTime):
    curTime = startTime
    second = timedelta(seconds = 1)

    count = random.randint(5,10)
    series = []
    value = 0;
    for _ in range(count):
        offset = random.randint(10, 600)
        length = random.randint(10, 600)
        flowSpeed = random.random()
        quantity = ceil(length*flowSpeed)
        curTime += second*offset
        for _ in range(quantity):
            curTime += second/flowSpeed;
            value += 1
            series.append({
                "timestamp": curTime.replace(microsecond=0),
                "id": sensorID,
                "name": sensorName,
                "value": "%.3f"%(value/1000.0)
            })
    return series

def generate(nSensors = 6):
    startTime = datetime.now().replace(microsecond=0)
    data = []
    for i in range(6):
        data.extend(_sensorSeries(i, "sensor%s"%i, startTime))
    data.sort(key = lambda dataEntry: dataEntry['timestamp'])
    return data
