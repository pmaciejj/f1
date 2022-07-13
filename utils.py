import pandas as pd
import requests
import os
import time
import json
import csv
from datetime import datetime, timedelta


main_path = os.path.dirname(os.path.realpath(__file__))
data_path =  os.path.join(main_path,"data")
races_path = os.path.join(data_path,"races")
qualifying_path =  os.path.join(data_path,"qualifying")
race_results_path = os.path.join(data_path,"race_results")
sprint_results_path = os.path.join(data_path,"sprint_results")


paths = (data_path,races_path,qualifying_path,qualifying_path,race_results_path,sprint_results_path)

for p in paths:
    if not os.path.isdir(p):
        os.mkdir(p)

request_counter_path =  os.path.join(main_path,"requests.csv")
if not os.path.isfile(request_counter_path):
    with open(request_counter_path,"w") as f:
        f.write("date\n")

ts = datetime.now()
ts_last_h = ts - timedelta(hours=1)
ts_last_h = datetime.strftime(ts_last_h,"%Y-%m-%d %H:%M:%S")

url = r"http://ergast.com/api/f1/"
s = requests.session()

def request_limit_read():
    r = pd.read_csv(request_counter_path,parse_dates = ["date"])
    r["diff"] =(r["date"] - pd.to_datetime(ts_last_h)).astype("timedelta64[m]").abs()
    limit = r["diff"].where(lambda x: x<= 60).count()
    return limit


def now_get() ->str:
    ts = datetime.now()
    return datetime.strftime(ts,"%Y-%m-%d %H:%M:%S")

def request_limit_add():
    with open(request_counter_path,"a+",newline="") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([now_get()])

def request_limit_exceeded() ->bool:
    if request_limit_read() > 190:
        return True
    return False



def races_get(year:int):
    # http://ergast.com/api/f1/2021.json
    file_path = races_path + "\\races_" + str(year) + ".json"
    if not os.path.isfile(file_path):
        time.sleep(4)
        if not request_limit_exceeded():
            url_races = url + str(year) + ".json"
            req = s.get(url_races)
            request_limit_add()
            if req.status_code == 200:
                with open(file_path,"w",encoding="utf-8") as f:
                    json.dump(json.loads(req.text),f,indent=4)

def qualifying_get(year:int,round:int):
    # http://ergast.com/api/f1/2021/1/qualifying.json
    file_path = qualifying_path + "\\qualifying_" + str(year) + "_" + str(round) + ".json"
    if not os.path.isfile(file_path):
        time.sleep(4)
        if not request_limit_exceeded():
            url_round = url + str(year) +"/" + str(round) +"/qualifying.json"
            req = s.get(url_round)
            #print(req.url)
            request_limit_add()
            if req.status_code == 200:
                with open(file_path,"w",encoding="utf-8") as f:
                    json.dump(json.loads(req.text),f,indent=4)

def race_result_get(year:int,round:int):
    # http://ergast.com/api/f1/2021/1/results.json
    file_path = race_results_path + "\\results_" + str(year) + "_" + str(round) + ".json"
    if not os.path.isfile(file_path):
        time.sleep(4)
        if not request_limit_exceeded():
            url_round = url + str(year) +"/" + str(round) +"/results.json" 
            req = s.get(url_round)
            #print(req.url)
            request_limit_add()
            if req.status_code == 200:
                with open(file_path,"w",encoding="utf-8") as f:
                    json.dump(json.loads(req.text),f,indent=4)

def sprint_result_get(year:int,round:int):
    # http://ergast.com/api/f1/2021/10/sprint
    file_path = sprint_results_path + "\\results_" + str(year) + "_" + str(round) + ".json"
    if not os.path.isfile(file_path):
        time.sleep(4)
        if not request_limit_exceeded():
            url_round = url + str(year) +"/" + str(round) +"/sprint.json" 
            req = s.get(url_round)
            #print(req.url)
            request_limit_add()
            if req.status_code == 200:
                with open(file_path,"w",encoding="utf-8") as f:
                    json.dump(json.loads(req.text),f,indent=4)