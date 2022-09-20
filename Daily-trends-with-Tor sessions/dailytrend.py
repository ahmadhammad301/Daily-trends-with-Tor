import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from collect import collect_data
import pytz
from Authentecation import google_credit,save_file
from dateutil.relativedelta import relativedelta
import gc
from writer import writer


tz_NY = pytz.timezone('America/New_York')

writer("startig at NY time: ",starting=True)

today = datetime.now().strftime("%Y-%m-%d")
last_month = datetime.now() - pd.DateOffset(months=12*10)
last_month = last_month.strftime("%Y-%m-%d")



master_df = pd.read_csv('https://storage.googleapis.com/public-quant/masterfile/entity_file.csv')
master_df["TICKER"].unique()[:5]
master_df["Search"] = master_df["TICKER"] + " Stock"



## choose/uncomment only one batch for every instance

# batch = master_df[0:3300]      #1
# filter_dir="google/daily1/"

#batch = master_df[3300:6600]   #2
#filter_dir="google/daily2/"

# batch = master_df[6600:9900]   #3
# filter_dir="google/daily3/"

# batch = master_df[9900:13200]  #4
# filter_dir="google/daily4/"

# batch = master_df[13200:16500] #5
# filter_dir="google/daily5/"

# batch = master_df[16500:19800] #6
# filter_dir="google/daily6/"

# batch = master_df[19800:23100] #7
# filter_dir="google/daily7/"

# batch = master_df[23100:26400] #8
# filter_dir="google/daily8/"

# batch = master_df[26400:29700] #9
# filter_dir="google/daily9/"

# batch = master_df[29700:]      #10
# filter_dir="google/daily10/"


today = datetime.now().strftime("%Y-%m-%d")
last_month = datetime.now() - pd.DateOffset(months=12*10)
last_month = last_month.strftime("%Y-%m-%d")


# uploading to google drive
client = google_credit()
bucket = "sovai-fast"
save_file("log.txt", filter_dir+"log.txt", client, bucket)

def check_files():
  blobs = client.list_blobs(bucket)
  all_files = [blob.name.split("/")[-1] for blob in blobs if filter_dir in blob.name ]
  return all_files

#check error files to not try download it again
with open('query.txt','a') as f:
  f.writelines("starting\n")

error_files=[line.strip() for line in open('query.txt')]
save_file("query.txt", filter_dir+"query.txt", client, bucket)

appender = []
for en, r in enumerate(batch["Search"].unique()):

  print(datetime.now(tz_NY).strftime("%m-%d %H:%M:%S") + f' getting file {r}\n')
  writer(f' getting file {r}')
  
  file_name = "goog_"+r.split(" ")[0].replace("/","-")+".csv"
  all_files = check_files()
  
  if file_name in all_files:
    print (datetime.now(tz_NY).strftime("%m-%d %H:%M:%S") + f' {r} file downloaded before')
    writer(f' {r} file downloaded before')
    continue

  if file_name in error_files:
    print (datetime.now(tz_NY).strftime("%m-%d %H:%M:%S") + f' {r} file has a value error before')
    writer(f' {r} file has a value error before')
    continue

  
  try:
    data = collect_data(r,start="2004-01-01", end=today,geo='', save=False, verbose=False)
  except ValueError as e:
    writer(f' there is a value error:{e}')
    save_file("log.txt", filter_dir+"log.txt", client, bucket)
    with open('query.txt','a') as f:
        f.writelines(f"{file_name}\n") 
    continue
  
  #saving the file
  data.to_csv(file_name)
  save_file(file_name, filter_dir+file_name, client, bucket)
  #saving log
  save_file("log.txt", filter_dir+"log.txt", client, bucket)
  os.remove(file_name)
  gc.collect()
  
  print(datetime.now(tz_NY).strftime("%m-%d %H:%M:%S") + ' done downloading file ',r)
  writer(f' done downloading file {r}')

writer(f"done scraping all files ")
writer ('+'*100)

