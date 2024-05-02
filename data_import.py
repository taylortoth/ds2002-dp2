from pymongo import MongoClient, errors
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
db = client.kne9xt
collection = db.file_contents

path = "data"

imported_count = 0
failed_count = 0
corrupted_count = 0

for (root, dirs, files) in os.walk(path):
    for file in files:
        file_path = os.path.join(root, file)
       
        try:
            with open(file_path) as f:
                file_data = json.load(f)
               
                if isinstance(file_data, list):
                    collection.insert_many(file_data)
                    imported_count += len(file_data)
                else:
                    collection.insert_one(file_data)
                    imported_count += 1
       
        except json.JSONDecodeError:
            corrupted_count += 1
            print(f"Corrupted JSON file: {file_path}")
       
        except errors.PyMongoError as e:
            failed_count += 1
            print(f"Failed to import: {file_path}")
            print(f"Error: {str(e)}")

print(f"Successfully imported documents: {imported_count}")
print(f"Failed to import documents: {failed_count}")
print(f"Corrupted documents: {corrupted_count}")

with open('count.txt', 'w') as f:
    f.write(f"Successfully imported documents: {imported_count}\n")
    f.write(f"Failed to import documents: {failed_count}\n")
    f.write(f"Corrupted documents: {corrupted_count}\n")