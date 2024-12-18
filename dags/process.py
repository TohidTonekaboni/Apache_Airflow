from pymongo import MongoClient
import clickhouse_connect as cc

def etl():
    #############################################################
    username = "root"
    password = "example"
    host = "mongodb-etl"  # Replace with your MongoDB server address
    port = 27017        # Replace with your MongoDB server port
    # MongoDB connection string with authentication
    uri = f"mongodb://{username}:{password}@{host}:{port}"
    # Create a MongoDB client
    client_mongo = MongoClient(uri)

    # Access the ETL database
    db = client_mongo.ETL

    # Access the Students collection
    students_collection = db.students
    # Get the number of documents in the 'students' collection
    document_count = students_collection.count_documents({})
    
    # Print the number of documents
    print(f"Number of documents in 'students' collection: {document_count}")
    #############################################################
    client_clickhouse = cc.get_client(
        host='clickhouse-etl',  # Replace with your ClickHouse host
        port=8123,         # Default port
        username='default',  # Replace with your ClickHouse username
        password=''          # Replace with your ClickHouse password
    )
    distinct_ids = client_clickhouse.query("SELECT DISTINCT(id) FROM airflow.students")
    distinct_id_list = [row[0] for row in distinct_ids.result_rows]
    # Query MongoDB for 10 random students whose id is not in the distinct_ids from ClickHouse
    pipeline = [
        {"$match": {"id": {"$nin": distinct_id_list}}},  # Exclude ids present in distinct_id_list
        {"$sample": {"size": 10}}  # Get 10 random documents
    ]
    # Run the aggregation pipeline
    random_students = students_collection.aggregate(pipeline)
    # Prepare data for insertion into ClickHouse
    student_data = []
    for student in random_students:
        # Ensure that the data is in the correct format
        student_data.append([
            int(student['id']),       
            str(student['name']), 
            int(student['age']) 
        ])
        
        

    # Insert data into ClickHouse students table
    if student_data:
        client_clickhouse.insert('airflow.students', student_data, column_names=['id','name','age']) 
        print(f"Inserted {len(student_data)} records into ClickHouse.")
    else:
        print("No data to insert.")
        