# Code adapted from: 
# https://www.mongodb.com/developer/languages/python/python-quickstart-aggregation/

import pymongo
import os
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pprint import pprint

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
    db = connect(db_name='sample_mflix')
    movies = db['movies']

    # Print one document
    result = movies.find_one({}, {'_id':0, 'title':1, 'plot':1})
    pprint(result)

    # Match->sort pipeline
    title = input('\n\nInput a title (Hit Enter to skip): ')
    if not title:
        title = 'A Star Is Born'
    
    # 1st pipeline stage
    stage_match_title = {
        "$match": {
         "title": title
        }
    }

    # 2nd stage
    stage_sort_year_ascending = {
        "$sort": { "year": pymongo.ASCENDING }
    }
    
    pipeline = [
        stage_match_title, 
        stage_sort_year_ascending,
    ]

    # Execute the pipeline
    results = movies.aggregate(pipeline)
    
    for movie in results:
        print(" * {title}, {first_castmember}, {year}".format(
                title=movie["title"],
                first_castmember=movie["cast"][0],
                year=movie["year"],
        ))

    # Look up related documents in the 'comments' collection:
    stage_lookup_comments = {
        "$lookup": {
                "from": "comments", 
                "localField": "_id", 
                "foreignField": "movie_id", 
                "as": "related_comments",
        }
    }

    # Limit to the first 5 documents:
    stage_limit_5 = { "$limit": 5 }

    pipeline = [
        stage_lookup_comments,
        stage_limit_5,
    ]

    results = movies.aggregate(pipeline)
    print('\n\nMovie comments:\n')
    for movie in results:
        print(f"Title: {movie['title']} \n Comments: {movie['related_comments']}\n")

    ### Grouping
    # Movie count by year
    stage_group_year = {
        "$group": {
                "_id": "$year",
                # Count the number of movies in the group:
                "movie_count": { "$count": { }}, 
        }
    }
    
    stage_sort_year_ascending = {
        "$sort": { "_id": pymongo.ASCENDING }
    }

    pipeline = [stage_group_year, stage_sort_year_ascending]
    results = movies.aggregate(pipeline)
    print('\n\nMovie count by year')
    for year in results:
        print(year)
