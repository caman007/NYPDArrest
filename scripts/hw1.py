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
