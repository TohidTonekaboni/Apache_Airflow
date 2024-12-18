from pymongo import MongoClient
import numpy as np

username = "root"
password = "example"
host = "localhost"  # Replace with your MongoDB server address
port = 27017        # Replace with your MongoDB server port

# MongoDB connection string with authentication
uri = f"mongodb://{username}:{password}@{host}:{port}"

# Create a MongoDB client
client = MongoClient(uri)

# Access the ETL database
db = client.ETL

# Access the Students collection
students_collection = db.students


for i in range(10000):
    student = {
        "id":i+1,
        "name": f"Tohid{i+1}",
        "age": np.random.randint(20,50)
    }
    insert_result = students_collection.insert_one(student)
    print(f"Inserted document with ID: {insert_result.inserted_id}")