import csv
import requests
import pandas as pd
import numpy as np


with requests.Session() as s:
    raw_data = s.get("https://data.cityofnewyork.us/resource/nc67-uf89.csv?$limit=50000&$offset=0")

    decoded_data = raw_data.content.decode("utf-8")
    data =  csv.reader(decoded_data.splitlines(), delimiter=",")
data = list(data)

headers = data[0]
data_rows = data[1:]

documents = [{header: row[idx] for idx, header in enumerate(headers)} for row in data_rows]
df = pd.DataFrame(documents)

df = df.replace("",np.nan)
df = df.dropna()
df = df.drop_duplicates()

df = df.astype( {
    "summons_number":np.int64,
    "issue_date": "datetime64[ns]",
    "violation_time":"datetime64[ns]",
    "judgment_entry_date":"datetime64[ns]",
    "fine_amount":"float",
    "interest_amount":"float",
    "penalty_amount":"float",
    "reduction_amount":"float",
    "payment_amount":"float",
    "amount_due":"float"
})

df["issue_day"] = df["issue_date"].dt.day_name()

def categorize_time(hour):
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 22:
        return "Evening"
    else:
        return "Night"
    
df["viol_day_time"] =   df['violation_time'].dt.hour.apply(categorize_time)