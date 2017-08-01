from datetime import datetime, timedelta
def ceil_dt(dt, res):
    # how many secs have passed this day
    nsecs = dt.hour*3600 + dt.minute*60 + dt.second + dt.microsecond*1e-6
    delta = res.seconds - nsecs % res.seconds
    if delta == res.seconds:
        delta = 0
    return dt + timedelta(seconds=delta)

def time_generator(start, stop, delta):
    curTime = start
    while curTime < stop:
        yield curTime
        curTime += delta



class Differentiator(object):
    """docstring for Differentiator."""

    def __init__(self, delta):
        super(Differentiator, self).__init__()
        self._delta = delta
        self._min = None
        self._max = None
        self._curTs = None
        self.data = []

    def ProcessData(self, timestamp, value):
        if not self._curTs:
            #First iteration
            self._min = value-0.001
            self._curTs = timestamp
        elif self._curTs != timestamp:
            #New time
            self.data.append({'timestamp': self._curTs, 'value': self._max - self._min})
            # Append zero values for others
            if timestamp:
                if (timestamp - self._curTs) > self._delta:
                    self.data.append({'timestamp': self._curTs+self._delta, 'value': 0})
                for time in time_generator(self._curTs+self._delta, timestamp, self._delta):
                    self.data.append({'timestamp': time, 'value': 0})
                self._curTs = timestamp
                self._min = self._max
        #Current minute
        self._max = value

def differentiate_data(rows):
    data = {
        'minute' : Differentiator(timedelta(minutes=1)),
        'hour' : Differentiator(timedelta(hours=1))
    }

    for row in rows:
        curTs = ceil_dt(row['timestamp'], timedelta(minutes=1))
        data['minute'].ProcessData(curTs, row['value']);
        curTs = ceil_dt(row['timestamp'], timedelta(hours=1))
        data['hour'].ProcessData(curTs, row['value']);
    return { key: val.data for key, val in data.items() }
