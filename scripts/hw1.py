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

