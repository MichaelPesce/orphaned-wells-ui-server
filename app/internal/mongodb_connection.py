import os
from dotenv import load_dotenv
import urllib.parse
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# fetch environment variables
load_dotenv()
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def connectToDatabase():
    username = urllib.parse.quote_plus(DB_USERNAME)
    password = urllib.parse.quote_plus(DB_PASSWORD)
    db_name = "uow"

    uri = f"mongodb+srv://{username}:{password}@cluster0.lh1kted.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri, server_api=ServerApi("1"))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command("ping")
        print("Successfully connected to MongoDB!")
    except Exception as e:
        print(f"unable to connect to db: {e}")

    db = client[db_name]
    return db
