# -*- coding: utf-8 -*-
"""
Created on Mon Oct  6 16:12:15 2025
@author: Admin
"""

import pymongo
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import gspread
import pytz
from pytz import timezone
import traceback
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def getCollection(collection_name):
    import os
    username = "ecotron_analytics"
    password = "ecotron_analytics123"

    if not username or not password:
        raise ValueError("MongoDB credentials not found in environment variables. Please set MONGO_USER and MONGO_PASS.")

    connection_string = f"mongodb+srv://{username}:{password}@eco.5vhr3.mongodb.net/eco"
    client = pymongo.MongoClient(connection_string)
    db = client["eco"]
    return db[collection_name]


def datetimeProcessingforUpload(data):
    df = data.copy()
    
    col_list = ['timestamp','starttime','endtime', 'rundate','insert_datetime','updatedAt', 'createdAt', 'start_time', 'end_time' ]
    for col in col_list:
        if col in df:
            df[col] = pd.to_datetime(df[col])
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('Asia/Kolkata')
    return df


def datetimeProcessingafterDownload(df1):
    df = df1.copy()
    col_list = ['deviceTime', 'timestamp', 'starttime', 'endtime', 'rundate',
                'startTime', 'endTime', 'createdAt']
    for col in col_list:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            df[col] = df[col].dt.tz_convert('Asia/Kolkata')
    return df

def pm_getData(collectionname, query):
    collection = getCollection(collectionname)
    result = list(collection.find(query))
    df = pd.DataFrame.from_records(result)
    if not df.empty:
        df = datetimeProcessingafterDownload(df)
    return df

# In[]

'''
Start time to calculate total run time
'''

start_time_log = datetime.now() # for calculate run_time into log_df

# In[]

"""Starttime and endtime
start time : from log data latest timestamp where status is success
end time : it is always latest 
"""

query = {"type": "OnGrid_report_metrics_log", "status": "success"}
collectionname = "SolarACStats"

df_new = pm_getData(collectionname, query)

rundate = datetime.today().date()

# Determine start_time
if not df_new.empty and "endtime" in df_new:
    starttime = df_new["endtime"].max()
else:
    starttime = datetime.combine(rundate - timedelta(days=1), time(23, 59, 59))

if starttime.tzinfo is None:
    starttime = pytz.timezone("Asia/Kolkata").localize(starttime)
    
starttime = starttime.astimezone(pytz.UTC)

# Determine end_time (always today's 18:29:59)
endtime = datetime.combine(rundate - timedelta(days=0), time(18, 29, 59))

# starttime = datetime.combine(rundate - timedelta(days=2), time(18, 30, 00))
# endtime = datetime.combine(rundate - timedelta(days=0), time(18, 29, 59))

print("Start Time:", starttime)
print("End Time:", endtime)

# In[]

#----------------------------HELPER FUNCTIONS------------------------------------
def getCollection(collectionname = "SolarACFaults"):
    username = "ecotron_analytics"
    password = "ecotron_analytics123"
    connection_string = "mongodb+srv://"+username+":"+password+"@eco.5vhr3.mongodb.net/eco"
    client = pymongo.MongoClient(connection_string)
    db = client["eco"]
    # collection = db["BatteryStats"]
    collection = db[collectionname]
    return collection

def datetimeProcessingafterDownload(df1):
    df = df1.copy()
    col_list = ['deviceTime', 'timestamp', 'starttime', 'endtime', 'rundate',
                'startTime', 'endTime', 'createdAt']
    for col in col_list:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            df[col] = df[col].dt.tz_convert('Asia/Kolkata')
    return df

def pm_getData(collectionname, query, projection=None):
    collection = getCollection(collectionname)
    result = list(collection.find(query, projection))
    df = pd.DataFrame.from_records(result)
    if not df.empty:
        df = datetimeProcessingafterDownload(df)
    return df

def push_df_to_mongodb(dataframe, collectionname):
    try:
        collection = getCollection(collectionname)
        data_dict = dataframe.to_dict(orient='records')
        if data_dict:
            collection.insert_many(data_dict)
            print(f"Data inserted successfully into '{collectionname}' collection.")
        else:
            print("No data to insert.")
    except Exception as e:
        print(f"Error while inserting: {e}")
        
# %%

#EZMCOGX000001

# ------------------------ MongoDB Query ------------------------------------------
query = {
    'Topic': {"$regex": '^EZMCOGX'},  # ^ indicates "starts with"
    'createdAt': {"$gte": starttime, "$lte": endtime}
}

# query = {'Topic': {"$in": ["EZMCSACX00020"]}, "createdAt": {"$gte": starttime, "$lte": endtime}}
# query = {"createdAt": {"$gte": starttime, "$lte": endtime}}
# fields_to_extract = ['Topic', 'createdAt', 'OP_W', 'GRID_V', 'PV_W', 'BATT_TYPE', 'BATT_CHG_I', 'BATT_DSCHG_I', 'BATT_V', 'W_STAT', 'BATT_CAP']

fields_to_extract = ['Topic', 'createdAt', 'PAC', 'CT_POWER', 'PV1_POWER', 'ENERGY_EXP_OUT', 'ENERGY_EXP_IN', 'ENERGY_TODAY_AC']
projection = {field: 1 for field in fields_to_extract}
projection['_id'] = 0
collectionname = "SolarAC"

df = pm_getData(collectionname, query, projection)
print(f"Data fetched: {len(df)} rows")

# If only one row, drop it (i.e., make df empty)
if len(df) == 1:
    print("Only one row found — removing it.")
    df = df.iloc[0:0]  # Creates an empty DataFrame with same columns

df = datetimeProcessingafterDownload(df)

start_time_logdf = df["createdAt"].min()
end_time_logdf = df["createdAt"].max()

# %%

def compute_solax_energy_stats(df, system_capacity_kWh=3.3):

    # --- Prepare dataframe ---
    df = df.sort_values(['Topic', 'createdAt']).reset_index(drop=True)
    df['createdAt'] = pd.to_datetime(df['createdAt'])
    df['Date'] = df['createdAt'].dt.date

    # --- Solar hours filter ---
    df['hour'] = df['createdAt'].dt.hour
    df = df[(df['hour'] >= 5) & (df['hour'] < 19)]

    # --- Logging interval detection ---
    df['prev_time'] = df.groupby('Topic')['createdAt'].shift(1)
    df['raw_deltatime'] = (df['createdAt'] - df['prev_time']).dt.total_seconds().fillna(0)
    logging_interval = df['raw_deltatime'].tail(100).median()
    if np.isnan(logging_interval) or logging_interval <= 0:
        logging_interval = df['raw_deltatime'].median()
    df['deltatime'] = np.where(df['raw_deltatime'] > 1.3 * logging_interval, logging_interval, df['raw_deltatime'])

    # --- Energy columns in Wh ---
    df['OP_Wh'] = df['ENERGY_TODAY_AC'] * 1000
    df['Grid_to_load_Wh'] = df['ENERGY_EXP_IN'] * 1000
    df['PV_to_grid_Wh'] = df['ENERGY_EXP_OUT'] * 1000
    df['PV_Wh'] = (df['PV1_POWER'] * df['deltatime']) / 3600

    # --- Function to sum positive diffs for cumulative columns ---
    def sum_cumulative_energy(x, t, system_capacity_kWh=3.3):
        """
        Sum valid increases in cumulative energy readings.
        Handles:
          - Negative dips (resets)
          - False 'recovery' spikes right after dips
          - Unrealistic spikes beyond physical capacity
        """
        diff = x.diff().fillna(0)
        dt = t.diff().dt.total_seconds().fillna(0)
    
        # Expected max possible Wh per interval
        max_energy_per_interval = system_capacity_kWh * 1000 * dt / 3600
        cap_factor = 20
    
        # Step 1: Identify negative dips
        dips = diff < 0
    
        # Step 2: Ignore negative diffs (resets)
        diff[dips] = 0
    
        # Step 3: Ignore next positive spike immediately after a dip (recovery jump)
        recovery_spikes = dips.shift(1, fill_value=False)
        diff[recovery_spikes] = 0
    
        # Step 4: Ignore unrealistic positive spikes
        diff = np.where(diff > max_energy_per_interval * cap_factor, 0, diff)
    
        return diff.sum()

    #     return diff.sum()
    def sum_cumulative_energy_op(x, t, system_capacity_kWh=3.3, typical_increment_W=100):
        """
        Sum positive diffs of OP_Wh (ENERGY_TODAY_AC), ignore resets,
        handle small increments, delayed updates, first-of-day values,
        and false recovery spikes.
        """
        x = x.reset_index(drop=True)
        t = pd.to_datetime(t).reset_index(drop=True)
    
        diff = x.diff().fillna(0)
        dt = t.diff().dt.total_seconds().fillna(0)
        max_energy_interval = system_capacity_kWh * 1000 * dt / 3600  # Wh per interval
    
        # --- Ignore negative dips ---
        dips = diff < 0
        diff[dips] = 0
    
        # --- Ignore recovery spikes after dips ---
        recovery_spikes = dips.shift(1, fill_value=False)
        diff[recovery_spikes] = 0
    
        # --- Cap excessively large diffs ---
        cap_factor = 20
        diff = np.where(diff > max_energy_interval * cap_factor,
                        max_energy_interval * cap_factor, diff)
    
        return diff.sum()

    # --- Aggregate daily per Topic ---
    grouped = df.groupby(['Topic', 'Date']).apply(
        lambda g: pd.Series({
            'PV_Wh': g['PV_Wh'].sum(),
            'OP_Wh': sum_cumulative_energy_op(g['OP_Wh'], g['createdAt']),
            'Grid_to_load_Wh': sum_cumulative_energy(g['Grid_to_load_Wh'], g['createdAt'], system_capacity_kWh),
            'PV_to_grid_Wh': sum_cumulative_energy(g['PV_to_grid_Wh'], g['createdAt'], system_capacity_kWh),
        })
    ).reset_index()

    # --- Derived metrics ---
    grouped['PV_to_load_Wh'] = grouped['OP_Wh'] - grouped['PV_to_grid_Wh']
    grouped['Total_Load_Wh'] = grouped['OP_Wh'] + grouped['Grid_to_load_Wh'] - grouped['PV_to_grid_Wh']

    # --- Convert Wh → kWh ---
    wh_cols = [c for c in grouped.columns if c.endswith('_Wh')]
    for c in wh_cols:
        grouped[c.replace('_Wh', '_kWh')] = (grouped[c] / 1000).round(3)
    grouped.drop(columns=wh_cols, inplace=True)

    return grouped
# %%

# df = pd.read_csv(input_folder)
df = datetimeProcessingafterDownload(df)
classified_df = compute_solax_energy_stats(df)
# push_df_to_mongodb(df_agg, collectionname='SolarACStats')
# %%

# df = pd.read_csv(input_folder)
df = datetimeProcessingafterDownload(df)
classified_df = compute_solax_energy_stats(df)

# In[]

'''
function to overwrite and push log and data into solarACDStats
'''

def push_logdf_to_mongodb(dataframe, collectionname):
    """
    Push a Pandas DataFrame to a MongoDB collection.
    
    Parameters:
    - dataframe: Pandas DataFrame containing the data to be pushed.
    - collectionname: Name of the MongoDB collection.
    
    Returns:
    - None
    """

    # Connect to MongoDB
    collection = getCollection(collectionname)
    # converting timestamp columns to pd.Timestamp type
    dataframe = datetimeProcessingforUpload(data = dataframe)
    # Convert DataFrame to dictionary
    data_dict = dataframe.to_dict(orient='records')

    # Insert data into MongoDB collection
    collection.insert_many(data_dict)

    print(f"Data inserted successfully into '{collectionname}' collection.")


def push_df_to_mongodb_overwrite(final_df, collectionname):
    """
    Push already-prepared final_df to MongoDB.
    Assumes keys & merges are already handled outside.
    """
    collection = getCollection(collectionname)

    # Convert to dict
    data_dict = final_df.to_dict(orient="records")

    # Upsert into MongoDB
    for doc in data_dict:
        filter_query = {"key": doc["key"]}
        collection.replace_one(filter_query, doc, upsert=True)

    print(f"Data upserted successfully into '{collectionname}' collection.")
    # print(f"Keys inserted/updated: {final_df['key'].tolist()}")

    return final_df["key"].tolist()

# In[]

classified_df['solar_flag'] = 'solar'

df = classified_df.copy()

# Add insert_datetime (Asia/Kolkata)
ist = pytz.timezone("Asia/Kolkata")
df["insert_datetime"] = datetime.now(ist)

# Add type column
df["type"] = "OnGrid_report_metrics"

# Ensure 'Date' is datetime
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"])

df["key"] = (
    df["Topic"].astype(str) + "_" +
    df["Date"].dt.strftime("%Y-%m-%d") + "_" +
    df["solar_flag"].astype(str) + "_solax"
)

# Collect keys
key_list = df["key"].tolist()

# Fetch old data for these keys
query = {"key": {"$in": key_list}, "type": "OnGrid_report_metrics" }
old_df = pm_getData("SolarACStats", query)

if "_id" in old_df.columns:
    old_df = old_df.drop(columns=["_id"])

# Merge old + new data if exists
if not old_df.empty:
    combined_df = pd.concat([old_df, df], ignore_index=True)

    # Identify numeric columns
    numeric_cols = [col for col in combined_df.select_dtypes(include="number").columns if col != "id"]

    # Aggregation rules
    agg_dict = {col: "sum" for col in numeric_cols}
    for col in combined_df.columns:
        if col not in numeric_cols + ["key"]:
            agg_dict[col] = "first"

    # Collapse duplicates
    final_df = combined_df.groupby("key", as_index=False).agg(agg_dict)
else:
    final_df = df

status = "success"
error_message = None

try:
    key_list = push_df_to_mongodb_overwrite(final_df, collectionname="SolarACStats")
except Exception as e:
    status = "failure"
    error_message = str(e)
    traceback.print_exc()

end_time_log = datetime.now()
run_time = (end_time_log - start_time_log).total_seconds() / 60  # minutes

# === Create log dataframe ===
df_log = pd.DataFrame([{
    "starttime":start_time_logdf,
    "endtime" :end_time_logdf,
    "shape": str(final_df.shape),
    "type": "OnGrid_report_metrics_log",
    "status": status,
    "error": error_message,
    "run_time": run_time,
    "insert_datetime": end_time_log
}])

push_logdf_to_mongodb(df_log, collectionname = "SolarACStats")






