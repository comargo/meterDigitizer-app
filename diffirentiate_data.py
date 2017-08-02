from datetime import datetime, timedelta
def ceil_dt(dt, res):
    # how many secs have passed this day
    nsecs = dt.hour*3600 + dt.minute*60 + dt.second + dt.microsecond*1e-6
    delta = res.seconds - nsecs % res.seconds
    if delta == res.seconds:
        delta = 0
    return dt + timedelta(seconds=delta)

def differentiate_data(rows, precision):
    _curTs = None
    _min = None
    _max = None
    for row in rows:
        timestamp = ceil_dt(row['timestamp'], precision);
        value = row['value'];
        if not _curTs:
            #First iteration
            _min = value-0.001
            _curTs = timestamp
        elif _curTs != timestamp:
            #New time
            yield {'timestamp': _curTs, 'value': round(_max - _min,3)}
            # Append zero values for others
            if timestamp:
                if (timestamp - _curTs) > precision:
                    yield {'timestamp': _curTs+precision, 'value': 0}
                _curTs = timestamp
                _min = _max
        #Current minute
        _max = value
