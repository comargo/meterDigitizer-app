class Differentiator(object):
    """docstring for Differentiator."""

    def __init__(self):
        super(Differentiator, self).__init__()
        self._min = None
        self._max = None
        self._curTs = None
        self.data = []

    def ProcessData(self, timestamp, value):
        if not self._curTs:
            #First iteration
            self._min = self._max = value
            self._curTs = timestamp
        elif self._curTs != timestamp:
            #New time
            self.data.append({'timestamp': self._curTs, 'value': self._max - self._min})
            self._curTs = timestamp
            self._min = self._max
        #Current minute
        self._max = value

    def ProcessLastData(self):
        self.ProcessData(None, None)

def differentiate_data(rows):
    data = {
        'minute' : Differentiator(),
        'hour' : Differentiator()
    }

    for row in rows:
        curTs = row['timestamp'].replace(second = 0, microsecond=0)
        data['minute'].ProcessData(curTs, row['value']);
        curTs = curTs.replace(minute = 0)
        data['hour'].ProcessData(curTs, row['value']);
    data['minute'].ProcessLastData();
    data['hour'].ProcessLastData();
    return { key: val.data for key, val in data.items() }
