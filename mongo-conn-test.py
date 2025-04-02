
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os 

load_dotenv()

MONGO_USER=os.environ.get('MONGO_USER')
MONGO_PASS=os.environ.get('MONGO_PASS')

# Replace the string below with the connection string that's provided to you by Atlas 
# uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}"+\
#    f"@dsc333.qmlmqnt.mongodb.net/?retryWrites=true&w=majority&appName=dsc333"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
