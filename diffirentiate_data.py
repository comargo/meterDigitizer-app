from datetime import datetime, timedelta

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
            self._min = value
            self._curTs = timestamp
        elif self._curTs != timestamp:
            #New time
            self.data.append({'timestamp': self._curTs, 'value': self._max - self._min})
            # Append zero values for others
            if timestamp:
                for time in time_generator(self._curTs+self._delta, timestamp, self._delta):
                    self.data.append({'timestamp': time, 'value': 0})
                self._curTs = timestamp
                self._min = self._max
        #Current minute
        self._max = value

    def ProcessLastData(self):
        self.ProcessData(None, None)

def differentiate_data(rows):
    data = {
        'minute' : Differentiator(timedelta(minutes=1)),
        'hour' : Differentiator(timedelta(hours=1))
    }

    for row in rows:
        curTs = row['timestamp'].replace(second = 0, microsecond=0)
        data['minute'].ProcessData(curTs, row['value']);
        curTs = curTs.replace(minute = 0)
        data['hour'].ProcessData(curTs, row['value']);
    data['minute'].ProcessLastData();
    data['hour'].ProcessLastData();
    return { key: val.data for key, val in data.items() }
