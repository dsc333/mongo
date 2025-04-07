import pymongo
import os
import pandas as pd
import getpass
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

MONGO_USER=os.environ.get('MONGO_USER')
MONGO_PASS=os.environ.get('MONGO_PASS')

# prompt user for username and password if not defined in .env
if not MONGO_USER:
    MONGO_USER = input('MongoDB username: ')
if not MONGO_PASS:
    MONGO_PASS = getpass.getpass()

def connect(db_name):
    # REPLACE THE DOMAIN WITH THE DOMAIN IN YOUR CONNECTION STRING
    uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}"+\
        f"@dsc333.qmlmqnt.mongodb.net/?retryWrites=true&w=majority&appName=dsc333"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    database = client[db_name]

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
    except Exception as e:
        print(e)
    
    return database


if __name__ == '__main__':
    db = connect(db_name='test')

    # Create cars collection from csv file if it doesn't exist
    if 'cars' not in db.list_collection_names():
        df = pd.read_csv('https://storage.googleapis.com/scsu-data-science/cars.csv')
        car_list = df.to_dict(orient='records')
        collection = database['cars']
        result = collection.insert_many(car_list)
    else:
        collection = db['cars']
    
    # Document count
    doc_count = collection.count_documents({})
    print(f'Document count: {doc_count}')

    # Distinct values
    print('\n\nDistinct car types')
    results = collection.distinct('Type')
    for doc in results:
        print(doc)

    # Find one matching document
    print('\n\nFirst match for Honda')
    honda = collection.find_one({'Name': {'$regex': '^Honda'}}, 
        {'_id':0, 'Name':1})
    print(honda)

    # Find all matching documents
    print('\n\nAll matches for Honda')
    results = collection.find({'Name': {'$regex': '^Honda'}}, 
        {'_id':0, 'Name':1})
    for car in results:
        print(car)

    # Find all matching documents and sort by mpg
    print('\n\nSort my mpg')
    results = (collection.find({'Name': {'$regex': '^Honda'}}, 
                {'_id':0, 'Name':1, 'Highway Miles Per Gallon':1})
                .sort('Highway Miles Per Gallon', pymongo.ASCENDING))
    for car in results:
        print(car)

    # Update a single document
    print('\n\nUpdate a document')
    query_filter = {'Name' : 'Honda Accord EX 2dr'}
    update_operation = { '$set' : 
        { 'Name' : 'Honda Accord EX 2dr (UPDATE)' }
    }
    collection.update_one(query_filter, update_operation)
    result = collection.find_one({'Name': 'Honda Accord EX 2dr (UPDATE)'},
        {'_id':0, 'Name':1})
    print(result)

    # Delete a document (and then reinsert it)
    print('\n\nDelete (and re-inserting) a document')
    query_filter = {'Name': {'$regex': '^Honda'} }
    result = collection.delete_one(query_filter)
    print(result)

    result = collection.insert_one(honda)
    print(result)



