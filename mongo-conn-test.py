
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os 
import getpass
import certifi

load_dotenv()

MONGO_USER=os.environ.get('MONGO_USER')
MONGO_PASS=os.environ.get('MONGO_PASS')

# prompt user for username and password if not defined in .env
if not MONGO_USER:
    MONGO_USER = input('MongoDB username: ')
if not MONGO_PASS:
    MONGO_PASS = getpass.getpass()

# Uncomment the connection string below and replace the domain dsc333.qmlmqnt.mongodb.net
# with your domain (check your connection string) and dsc333 at the end with 
# your cluster name.  

# uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}"+\
#    f"@dsc333.qmlmqnt.mongodb.net/?retryWrites=true&w=majority&appName=dsc333"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
