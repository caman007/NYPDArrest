import csv
import requests
import pymongo
import mysql.connector

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["parking_violations"] 
collection = database["violations"] 

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="654239"
)

with requests.Session() as s:
    raw_data = s.get("https://data.cityofnewyork.us/resource/nc67-uf89.csv?$limit=50000&$offset=0")

    decoded_data = raw_data.content.decode("utf-8")
    data =  csv.reader(decoded_data.splitlines(), delimiter=",")
data = list(data)

headers = data[0]
data_rows = data[1:]

documents = [{header: row[idx] for idx, header in enumerate(headers)} for row in data_rows]

collection.insert_many(documents)
print("Data Inserted in MongoDB Successfully.")

cursor = connection.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS violations")
cursor.execute("USE violations")


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
    "issue_date VARCHAR(255), "
    "violation_time VARCHAR(255), "
    "judgment_entry_date VARCHAR(255), "
    "summons_image VARCHAR(255) "
)

fact_table_schema = (
    "fine_amount VARCHAR(255), "
    "penalty_amount VARCHAR(255), "
    "interest_amount VARCHAR(255), "
    "reduction_amount VARCHAR(255), "
    "payment_amount VARCHAR(255), "
    "amount_due VARCHAR(255), "
    "incidentID INT "
)

# create dimension table
create_dimension_table_query = f"CREATE TABLE IF NOT EXISTS incident (incidentID INT AUTO_INCREMENT PRIMARY KEY, {dimension_table_schema})"
cursor.execute(create_dimension_table_query)
connection.commit()
print("Dimensional Table Created")

# Create fact table
create_fact_table_query = f"CREATE TABLE IF NOT EXISTS penalty ({fact_table_schema})"
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
    "summons_image VARCHAR(255), "
]

counter = 1
for row in documents:


    dim_columns = ", ".join([item.split(" ")[0] for item in dimension_table_columns]) 
    dim_query = f"INSERT INTO incident ({dim_columns}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(dim_query,([row[item.split(" ")[0]] for item in dimension_table_columns]))
    print("dim inserted")

    fact_columns = ", ".join([item.split(" ")[0] for item in fact_table_columns]) 
    fact_query = f"INSERT INTO penalty ({fact_columns}) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(fact_query,([row[item.split(" ")[0]] for item in fact_table_columns[:-1]]+[counter]))
    print("fact inserted")
    
    connection.commit()
    counter +=1
