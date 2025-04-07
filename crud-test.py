import pymongo
import os
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

MONGO_USER=os.environ.get('MONGO_USER')
MONGO_PASS=os.environ.get('MONGO_PASS')

def connect(db_name):
    # replace with your connection string
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

    # Find one matching document
    print('\n\nFirst match for Honda')
    result = collection.find_one({'Name': {'$regex': '^Honda'}}, {'_id':0, 'Name':1})
    print(result)

    # Find all matching documents
    print('\n\nAll matches for Honda')
    results = collection.find({'Name': {'$regex': '^Honda'}}, {'_id':0, 'Name':1})
    for car in results:
        print(car)

    # Find all matching documents and sort by mpg
    print('\n\nSort my mpg')
    results = (collection.find({'Name': {'$regex': '^Honda'}}, {'_id':0, 'Name':1, 'Highway Miles Per Gallon':1})
                .sort('Highway Miles Per Gallon', pymongo.ASCENDING))
    for car in results:
        print(car)


# Update
'''
restaurants = database["restaurants"]

query_filter = {'name' : 'Bagels N Buns'}
update_operation = { '$set' : 
    { 'name' : '2 Bagels 2 Buns' }
}

result = restaurants.update_one(query_filter, update_operation)
'''

