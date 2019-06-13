from datetime import datetime, timedelta
import dateutil.parser as dp
import pytz
import re

raise_ = object()
marker = object()

def asduration(value, default=raise_, asint=False):
    def out(value):
        if value is marker:
            if default is raise_:
                raise ValueError('not a duration value')
            return default
        if asint:
            return int(value.total_seconds())
        return value

    if value is None:
        return out(marker)

    if isinstance(value, timedelta):
        return out(value)

    if isinstance(value, int):
        return out(timedelta(seconds=value))

    m = re.match(r'\s*(\d+(?:\.\d*)?)\s*([DdHhMmSs])?', value)
    if m is None:
        return out(marker)

    n = float(m.groups()[0])
    what = m.groups()[1] or 's'

    if what.lower() == 'm':
        return out(timedelta(minutes=n))
    if what.lower() == 'h':
        return out(timedelta(hours=n))
    if what.lower() == 'd':
        return out(timedelta(days=n))
    return out(timedelta(seconds=n))

def astimestamp(value, default=raise_):
    def out(value):
        if value is marker:
            if default is raise_:
                raise ValueError('not a timestamp')
            return default
        return value

    if value is None:
        return out(marker)

    if isinstance(value, datetime):
        return out(value)

    try:
        result = dp.parse(value)
    except Exception:
        return out(marker)
    if result.tzinfo is not None:
        result = result.astimezone(pytz.utc).replace(tzinfo=None)
    return out(result)
