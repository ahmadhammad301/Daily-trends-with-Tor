import io
from datetime import datetime, date
from ssl import SSL_ERROR_SSL
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
import Authentecation
tz_NY = pytz.timezone('America/New_York')

proxies = {'http': 'socks5://127.0.0.1:9050'
            ,'https': 'socks5://127.0.0.1:9050'}


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
    session.proxies = proxies
    
    

    try:
        jar = session.get("https://trends.google.com/").cookies
        
    except: 
        writer(' error in session.get')
        renew_connection()
        time.sleep(3)
        jar = session.get("https://trends.google.com/").cookies
    
    
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar),urllib.request.ProxyHandler(proxies))

    
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
    # print('\n opener ip',opener.open("https://icanhazip.com/").read())
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

def get_tor_session():
    session = requests.session()
    # Tor uses the 9050 port as the default socks port
    session.proxies = {'http':  'socks5://127.0.0.1:9050',
                       'https': 'socks5://127.0.0.1:9050'}
    return session


# signal TOR for a new connection
def renew_connection():
    
    print("new Identity requested")
    writer("new Identity requested",starting=False)
    session = get_tor_session()
    try:
        old_ip=session.get("https://icanhazip.com/").text.strip()
    except:
        writer("exceptuion occured in icanhazip.com")
        print("exceptuion occured in icanhazip.com")    
        old_ip=session.get("https://checkip.amazonaws.com/").text.strip()

    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password=Authentecation.Tor_password)
        controller.signal(Signal.NEWNYM)
        time.sleep(1)
        session = get_tor_session()
        
        try:
            new_ip=session.get("https://icanhazip.com/").text.strip()
        except:
            writer("exceptuion occured in icanhazip.com")
            print("exceptuion occured in icanhazip.com")
            new_ip=session.get("https://checkip.amazonaws.com/").text.strip()
    tries = 0        
    while old_ip==new_ip:
        tries += 1
        if tries>5:
            renew_connection()
        print("trying again")
        time.sleep(1)
        session = get_tor_session()
        try:
            new_ip=session.get("https://icanhazip.com/").text
        except:
            writer("exceptuion occured in icanhazip.com")
            print("exceptuion occured in icanhazip.com")        
            new_ip=session.get("https://checkip.amazonaws.com/").text.strip()

    
    print("new Ip: ",new_ip)
    writer("new Ip: "+str(new_ip))


def collect_frames(q:None, start:str, end:str, geo:str) -> list:
    intervals = generate_intervals(init_start=start, init_end=end)
    frames = []
    #renew_connection()
       
    for idx ,interval in enumerate(tqdm(intervals)):
        # renew_connection() #uncomment if you want to renew your ip every request (not recommended)
        flag=False
        tries=0
        while not flag:                
            try:
                df = get_frame(q, interval, geo)
                flag=True
                tries=0 ## i need to give the ip another try
            except urllib.error.HTTPError as e :
                tries +=1
                writer(f' getting blocked from the server... handling it..{str(e)}')
                time.sleep(random.gammavariate(0.99,2.99))
                if tries>=2:
                    renew_connection()
            except ConnectionResetError as e:
                print(f' ConnectionResetError{str(e)}')
                writer(f' ConnectionResetError{str(e)} ')
                if tries>=2:
                    renew_connection()
            except requests.exceptions.SSLError as e :
                writer(f' error in requests.exceptions.SSLError{str(e)} ')
                time.sleep(random.gammavariate(0.99,2.99))
                if tries>=2:
                    renew_connection()
            except urllib.error.URLError as e :                
                writer(f'  urllib.error.URLError {str(e)}')
                time.sleep(random.gammavariate(0.99,2.99))
                if tries>=2:
                    renew_connection()
            except Exception as e :
                writer(f' unexpected error: {str(e)}')
                time.sleep(random.gammavariate(0.99,2.99))
                if tries>=2:
                    renew_connection()

        time.sleep(random.gammavariate(.99,2.99)) # I should try to make a new condition > 3 seconds
        if len(df) == 0:
            continue
        frames.append(df)
    return frames

