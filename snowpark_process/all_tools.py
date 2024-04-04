
from datetime import datetime
def get_readable_curtime(seconds):
    return datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')