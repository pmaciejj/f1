import pandas as pd
import json
import os
from glob import glob

import utils

seasons = [2020,2021,2022]

# download json with races per season
for s in seasons:
    utils.races_get(year=s)

season_round = []
season_round_sprint =[]

# create csv with all races
for i,f in enumerate(glob(utils.races_path + "/*json")):
    with open(f,"r") as f:
        data = json.load(f)
    df = pd.json_normalize(data,record_path=['MRData','RaceTable',"Races"],sep="_")
    df["date"] = pd.to_datetime(df["date"],format="%Y-%m-%d")
    cols_to_del = ["url","time","Circuit_url","Circuit_Location_lat","Circuit_Location_long","FirstPractice_date","FirstPractice_time","SecondPractice_date","SecondPractice_time","ThirdPractice_date","ThirdPractice_time","Qualifying_date","Qualifying_time","Sprint_time"]
    ## try except because respose is diffrent depending on year
    for col in cols_to_del:
        try:
            df.drop(columns=col,inplace=True)
        except:
            pass

    if "Sprint_date" not in df.columns.to_list():
        df["Sprint_date"] = None

    # dataframe with races with results
    sr_df = df[df["date"] < pd.to_datetime('today')]
    for s_r in  (sr_df["season"] +"_"+  sr_df["round"]).to_list():
        season_round.append(s_r)



    sprints = sr_df[sr_df["Sprint_date"].notna()]
    for sprint_r in  (sprints["season"] +"_"+  sprints["round"]).to_list():
        season_round_sprint.append(sprint_r)


    races_csv = os.path.join(utils.data_path,"races.csv")
    if i == 0:
        df.to_csv(races_csv,index=False,mode="w",)
    else:
        df.to_csv(races_csv,index=False,mode="a",header=False)

# download json with quali results per season,round
for s_r in season_round:
    season = s_r.split("_")[0]
    round = s_r.split("_")[1]
    utils.qualifying_get(season,round)

# create csv with all quali results
for i,f in enumerate(glob(utils.qualifying_path + "/*json")):

    with open(f,"r") as f:
        data = json.load(f)

    s = data["MRData"]["RaceTable"]["season"]
    r = data["MRData"]["RaceTable"]["round"]

    df = pd.json_normalize(data,record_path=['MRData','RaceTable',"Races","QualifyingResults"],sep="_")
    cols_to_del = ["Driver_url","Driver_dateOfBirth","Constructor_url","Constructor_nationality","Driver_nationality"]

    for col in cols_to_del:
        try:
            df.drop(columns=col,inplace=True)
        except:
            pass

    df.insert(0,"round",r)
    df.insert(0,"season",s)
    
    qualifying_csv = os.path.join(utils.data_path,"qualifying.csv")
    if i == 0:

        df.to_csv(qualifying_csv,index=False,mode="w",)
    else:
        df.to_csv(qualifying_csv,index=False,mode="a",header=False)

# download json with race results per season,round
for s_r in season_round:
    season = s_r.split("_")[0]
    round = s_r.split("_")[1]
    utils.race_result_get(season,round)

# create csv with all results
for i,f in enumerate(glob(utils.race_results_path + "/*json")):

    with open(f,"r") as f:
        data = json.load(f)

    s = data["MRData"]["RaceTable"]["season"]
    r = data["MRData"]["RaceTable"]["round"]

    df = pd.json_normalize(data,record_path=['MRData','RaceTable',"Races","Results"],sep="_")
    cols_to_del = ["Driver_url","Constructor_url","Constructor_nationality","FastestLap_AverageSpeed_units","Driver_nationality","Driver_dateOfBirth"]

    for col in cols_to_del:
        try:
            df.drop(columns=col,inplace=True)
        except:
            pass

    df.insert(0,"round",r)
    df.insert(0,"season",s)
    
    results_csv = os.path.join(utils.data_path,"results.csv")
    if i == 0:

        df.to_csv(results_csv,index=False,mode="w",)
    else:
        df.to_csv(results_csv,index=False,mode="a",header=False)


# download jsn with sprint results
for s_r in  season_round_sprint:
    season = s_r.split("_")[0]
    round = s_r.split("_")[1]
    utils.sprint_result_get(season,round)

for i,f in enumerate(glob(utils.sprint_results_path + "/*json")):

    with open(f,"r") as f:
        data = json.load(f)

    s = data["MRData"]["RaceTable"]["season"]
    r = data["MRData"]["RaceTable"]["round"]

    df = pd.json_normalize(data,record_path=['MRData','RaceTable',"Races","SprintResults"],sep="_")
    cols_to_del = ["Driver_url","Constructor_url","Constructor_nationality","FastestLap_AverageSpeed_units","Driver_nationality","Driver_dateOfBirth"]

    for col in cols_to_del:
        try:
            df.drop(columns=col,inplace=True)
        except:
            pass

    df.insert(0,"round",r)
    df.insert(0,"season",s)
    
    sprint_results_csv = os.path.join(utils.data_path,"sprint_results.csv")
    if i == 0:

        df.to_csv(sprint_results_csv,index=False,mode="w")
    else:
        df.to_csv(sprint_results_csv,index=False,mode="a",header=False)


### create csvs (db tabels like) for power bi report 
races = pd.read_csv(utils.data_path+"\\races.csv")
quali = pd.read_csv(utils.data_path+"\\qualifying.csv")
results = pd.read_csv(utils.data_path+"\\results.csv")
sprints = pd.read_csv(utils.data_path+"\\sprint_results.csv")

calendar = races[["season","round","date","Circuit_circuitId"]]
tracks = races[["Circuit_circuitId","Circuit_circuitName","Circuit_Location_locality"]]
drivers = results[["Driver_driverId","Driver_code","Driver_givenName","Driver_familyName"]].drop_duplicates()
teams =results[["Constructor_constructorId","Constructor_name"]].drop_duplicates()
driver_team = results[["season","Driver_driverId","Constructor_constructorId"]].drop_duplicates()
sprint_results = sprints[["season","round","Driver_driverId","points"]]
race_results = results[["season","round","Driver_driverId","grid","position","positionText","points","FastestLap_rank"]]


results =  pd.merge(race_results,sprint_results,how = "left", left_on=["season","round","Driver_driverId"], right_on=["season","round","Driver_driverId"],suffixes=["","_sprint"])
results["points_sprint"].fillna(0,inplace=True)

# calendar
# tracks
# drivers
# teams
# driver_team
# sprint_results
# race_results
tables_names = ["calendar","tracks","drivers","teams","driver_team","results",]
tables = [calendar,tracks,drivers,teams,driver_team,results]

for i,t in enumerate(tables):
    t_n = tables_names[i]
    t_path = os.path.join(utils.main_path,"PowerBI_report","tabs",t_n + ".csv")
    t.to_csv(t_path,index=False,mode="w")
