import pytz
from datetime import datetime
tz_NY = pytz.timezone('America/New_York')
datetime_NY = datetime.now(tz_NY)

def ny_date():
    return str(datetime.now(tz_NY).strftime("%m-%d %H:%M:%S"))

def writer(msg,starting=False):
    if starting:
        with open('log.txt','a') as f:
            f.write('*'*50+'\n\n')

    with open('log.txt','a') as f:
        f.write(ny_date() + f'  {msg}\n')

