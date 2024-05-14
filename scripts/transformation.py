import csv
import requests
import pandas as pd
import numpy as np
import mysql.connector

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="654239"
)
cursor = connection.cursor()


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
df["year"] = df["issue_date"].dt.year

data_t = df.to_dict("records")

print(data_t[0])
cursor.execute("CREATE DATABASE IF NOT EXISTS violations_trans")
cursor.execute("USE violations_trans")

dimension_table_schema=(
    "plate VARCHAR(255), "
    "state VARCHAR(255), "
    "license_type VARCHAR(255), "
    "violation VARCHAR(255), "
    "summons_number VARCHAR(255), "
    "precinct VARCHAR(255), "
    "county VARCHAR(255), "
    "issuing_agency VARCHAR(255), "
    "violation_status VARCHAR(255), "
    "issue_date DATETIME, "
    "violation_time DATETIME, "
    "judgment_entry_date DATETIME, "
    "summons_image VARCHAR(255), "
    "year INT, "
    "viol_day_time VARCHAR(255), "
    "issue_day VARCHAR(255) "
    
)

fact_table_schema = (
    "fine_amount FLOAT, "
    "penalty_amount FLOAT, "
    "interest_amount FLOAT, "
    "reduction_amount FLOAT, "
    "payment_amount FLOAT, "
    "amount_due FLOAT, "
    "incidentID INT "
)

# create dimension table
create_dimension_table_query = f"CREATE TABLE IF NOT EXISTS incident_t (incidentID INT AUTO_INCREMENT PRIMARY KEY, {dimension_table_schema})"
cursor.execute(create_dimension_table_query)
connection.commit()
print("Dimensional Table Created")

# Create fact table
create_fact_table_query = f"CREATE TABLE IF NOT EXISTS penalty_t ({fact_table_schema})"
cursor.execute(create_fact_table_query)
connection.commit()
print("Fact Table Created")


fact_table_columns = [
    "fine_amount VARCHAR(255), ",
    "penalty_amount VARCHAR(255), ",
    "interest_amount VARCHAR(255), ",
    "reduction_amount VARCHAR(255), ",
    "payment_amount VARCHAR(255), ",
    "amount_due VARCHAR(255), ",
    "incidentID INT, "
]

dimension_table_columns = [
    "plate VARCHAR(255), ",
    "state VARCHAR(255), ",
    "license_type VARCHAR(255), ",
    "violation VARCHAR(255), ",
    "summons_number VARCHAR(255), ",
    "precinct VARCHAR(255), ",
    "county VARCHAR(255), ",
    "issuing_agency VARCHAR(255), ",
    "violation_status VARCHAR(255), ",
    "issue_date VARCHAR(255), ",
    "violation_time VARCHAR(255), ",
    "judgment_entry_date VARCHAR(255), ",
    "summons_image VARCHAR(255), ",
    "year INT ",
    "viol_day_time VARCHAR(255) ",
    "issue_day VARCHAR(255) ",
]

counter = 1
for row in data_t:
    dim_columns = ", ".join([item.split(" ")[0] for item in dimension_table_columns]) 
    dim_query = f"INSERT INTO incident_t ({dim_columns}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(dim_query,([row[item.split(" ")[0]] for item in dimension_table_columns]))
    print("dim inserted")

    fact_columns = ", ".join([item.split(" ")[0] for item in fact_table_columns]) 
    fact_query = f"INSERT INTO penalty_t ({fact_columns}) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(fact_query,([row[item.split(" ")[0]] for item in fact_table_columns[:-1]]+[counter]))
    print("fact inserted")
    
    connection.commit()
    counter +=1
