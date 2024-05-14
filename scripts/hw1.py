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
    user="user",
    password="pass"
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

collection.insert_many(documents)
print("Data Inserted in MongoDB Successfully.")


cursor.execute("CREATE DATABASE IF NOT EXISTS violations")
cursor.execute("USE violations")

dimension_schemas = {
    
    "state": "state VARCHAR(255)",
    "license_type": "license_type VARCHAR(255)",
    "violation": "Violation VARCHAR(255)",
    "precinct": "precinct VARCHAR(255)",
    "county": "county VARCHAR(255)",
    "issuing_agency": "issuing_agency VARCHAR(255)",
    "violation_status": "violation_status VARCHAR(255)"
}

for dimension, schema in dimension_schemas.items():
    create_dimension_table_query = f"CREATE TABLE IF NOT EXISTS {dimension} ({dimension}ID INT AUTO_INCREMENT PRIMARY KEY, {schema})"
    cursor.execute(create_dimension_table_query)
    connection.commit()


fact_table_schema = (
    "summons_number VARCHAR(255), "
    "issue_date VARCHAR(255), "
    "violation_time VARCHAR(255), "
    "violation VARCHAR(255), "
    "judgment_entry_date VARCHAR(255), "
    "fine_amount VARCHAR(255), "
    "penalty_amount VARCHAR(255), "
    "interest_amount VARCHAR(255), "
    "reduction_amount VARCHAR(255), "
    "payment_amount VARCHAR(255), "
    "amount_due VARCHAR(255), "
    "precinct VARCHAR(255), "
    "county VARCHAR(255), "
    "issuing_agency VARCHAR(255), "
    "violation_status VARCHAR(255), "
    "summons_image VARCHAR(255), "
    "countyID INT, "
    "issuing_agencyID INT, "
    "stateID INT, " 
    "violationID INT, "
    "precinctID INT, "
    "license_typeID INT, "
    "violation_statusID INT"
)

# Create fact table
create_fact_table_query = f"CREATE TABLE IF NOT EXISTS traffic_violations ({fact_table_schema})"
cursor.execute(create_fact_table_query)
connection.commit()


fact_table_columns = [
    "summons_number VARCHAR(255), ",
    "issue_date VARCHAR(255), ",
    "violation_time VARCHAR(255), ",
    "violation VARCHAR(255), ",
    "judgment_entry_date VARCHAR(255), ",
    "fine_amount VARCHAR(255), ",
    "penalty_amount VARCHAR(255), ",
    "interest_amount VARCHAR(255), ",
    "reduction_amount VARCHAR(255), ",
    "payment_amount VARCHAR(255), ",
    "amount_due VARCHAR(255), ",
    "precinct VARCHAR(255), ",
    "county VARCHAR(255), ",
    "issuing_agency VARCHAR(255), ",
    "violation_status VARCHAR(255), ",
    "summons_image VARCHAR(255)",
]


counter = 1
for row in documents:
    query = f"INSERT INTO state (state) VALUES (%s)"
    cursor.execute(query,(row["state"],))

    query = f"INSERT INTO license_type (license_type) VALUES (%s)"
    cursor.execute(query,(row["license_type"],))
    
    query = f"INSERT INTO violation (violation) VALUES (%s)"
    cursor.execute(query,(row["violation"],))

    query = f"INSERT INTO precinct (precinct) VALUES (%s)"
    cursor.execute(query,(row["precinct"],))

    query = f"INSERT INTO county (county) VALUES (%s)"
    cursor.execute(query,(row["county"],))

    query = f"INSERT INTO issuing_agency (issuing_agency) VALUES (%s)"
    cursor.execute(query,(row["issuing_agency"],))

    query = f"INSERT INTO violation_status (violation_status) VALUES (%s)"
    cursor.execute(query,(row["violation_status"],))

    columns = ", ".join([item.split(" ")[0] for item in fact_table_columns]+ ["countyID", "issuing_agencyID", "stateID", "violationID", "precinctID", "license_typeID", "violation_statusID"]) 
    query = f"INSERT INTO traffic_violations ({columns}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(query,([row[item.split(" ")[0]] for item in fact_table_columns]+[counter,counter,counter,counter,counter,counter,counter]))
    connection.commit()
    counter +=1
