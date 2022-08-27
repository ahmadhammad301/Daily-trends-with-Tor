import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta as delta
import requests
import urllib
import json
import random
import pandas as pd
import numpy as np
import time
from tqdm import tqdm
from stem.control import Controller
from stem import Signal
import extproxy
import pytz
from writer import writer
tz_NY = pytz.timezone('America/New_York')



def generate_intervals(overlap:int=35,
                       inc:int=250,
                       init_start:str="2004-01-01",
		       init_end:str="TODAY") -> list:
    """ 
    start defaults to "2004-01-01", which represents the entire series.
    Format : "YYYY-MM-DD"  
    """

    to_str = lambda dt_date : datetime.strftime(dt_date, "%Y-%m-%d")
    to_dt = lambda str_date : datetime.strptime(str_date, "%Y-%m-%d")
    intervals = []
    if init_end == "TODAY":
        init_end = to_dt(to_str(date.today()))
    else:
        init_end = to_dt(init_end)
    init_start = to_dt(init_start)
    duration = init_end - init_start
    n_iter = int(duration.days / (inc - overlap))
    if n_iter == 0:
        return [to_str(init_start) + " " + to_str(init_end)]
    for i in range(n_iter):
        # Start(i) < End(i-1)
        # End(i) > Start(i+1)
        if i == 0:
            end = to_str(init_end)
            start = to_str(to_dt(end) - delta(days=+inc))
        else:
            last = intervals[i-1]
            last_start, last_end = last[:10], last[11:]
            end = to_str(to_dt(last_start) + delta(days=+overlap))[:10]
            start = to_str(to_dt(end) - delta(days=+inc))
            
        intervals.append(start + " " + end)
        
    intervals.reverse()
    return intervals

def get_frame(q:None, time_:str, geo:str) -> pd.DataFrame:
    q = [q] if type(q) == str else q

    
    session = requests.session()
    session.proxies = {'http':  'socks5://127.0.0.1:9050'
                       ,'https': 'socks5://127.0.0.1:9050'}
    proxies = {'http':  'socks5://127.0.0.1:9050'
                       ,'https': 'socks5://127.0.0.1:9050'}

    try:
        jar = session.get("https://trends.google.com/").cookies
    except: 
        writer(' error in session.get')
        time.sleep(20)
        jar = session.get("https://trends.google.com/").cookies
    urllib.request.ProxyHandler(proxies)
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = [
                ("Referrer", "https://trends.google.com/trends/explore"),
                ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'),
                ("Accept", "text/plain") ]
    params_1 = None
    params_0 = {
                            "hl": "",
                            "tz": -120,
                            "req": {
                            "comparisonItem":[{'keyword': query, 'geo': geo, 'time': time_} for query in q] ,
                                    "category": 0,
                                    "property": "" }}
    params_0["req"] = json.dumps(params_0["req"], separators=(',', ':')) 
    params_0 = urllib.parse.urlencode(params_0)
    params_0 = params_0.replace('%3A', ':').replace('%2C', ',')  
    urllib.request.install_opener(opener)
    data = opener.open("https://trends.google.com/trends/api/explore?" + params_0).read().decode('utf8')
    data = data[data.find("{"):]
    data = json.loads(data)
    widgets = data["widgets"]


    for widget in widgets:
        if widget["title"] == "Interest over time":
            params_1 = {
                                            "req":widget["request"],
                                            "token":widget["token"],
                                            "tz":-120
                                    }
    params_1["req"] = json.dumps(params_1["req"],separators=(',', ':'))
    params_1 = urllib.parse.urlencode(params_1).replace("+", "%20")
    csv_url = 'https://trends.google.com/trends/api/widgetdata/multiline/csv?' + params_1
    result = opener.open(csv_url).read().decode('utf8')
    df = pd.read_csv(io.StringIO(result), skiprows=range(0,1), index_col=0, header=0)
    df.index = pd.DatetimeIndex(df.index)
    return df.asfreq("d")

def _renew_connection():
    with Controller.from_port(port = 9051) as controller:
                    controller.authenticate(password="Upwork")
                    controller.signal(Signal.NEWNYM)

def collect_frames(q:None, start:str, end:str, geo:str) -> list:
    intervals = generate_intervals(init_start=start, init_end=end)
    frames = []
    _renew_connection()   
    for idx ,interval in enumerate(tqdm(intervals)):               
        
        flag=False
        while not flag:                
            try:
                df = get_frame(q, interval, geo)
                flag=True
            except urllib.error.HTTPError as e :
                writer(f' getting blocked from the server... handling it..{str(e)} \n')

                time.sleep(random.gammavariate(1.99,3.99))
                _renew_connection()
                # df = get_frame(q, interval, geo) 
            except requests.exceptions.SSLError as e :
                writer(f' error in requests.exceptions.SSLError{str(e)} \n')
                time.sleep(random.gammavariate(1.99,3.99))
                _renew_connection()
                # df = get_frame(q, interval, geo)
            except urllib.error.URLError as e :                
                writer(f' error in requests.exceptions.SSLError {str(e)}\n')
                time.sleep(random.gammavariate(1.99,3.99))
                _renew_connection()
                # df = get_frame(q, interval, geo)

        time.sleep(random.gammavariate(.99,2.99)) # I should try to make a new condition > 3 seconds
        if len(df) == 0:
            continue
        frames.append(df)
    return frames

