import os
from pymongo import MongoClient
from pymongo.database import Database
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

MONGO_DETAILS = os.getenv("MONGO_DETAILS")
DB_NAME = "bse_analysis"

if not MONGO_DETAILS:
    raise ValueError("MONGO_DETAILS not found in .env file. Please set it.")

class DBMongo:
    client: MongoClient = None
    db: Database = None

db_mongo = DBMongo()

def connect_to_mongo():
    """Establishes connection to MongoDB."""
    #print("Connecting to MongoDB...")
    db_mongo.client = MongoClient(MONGO_DETAILS)
    db_mongo.db = db_mongo.client[DB_NAME]
    #print(f"Successfully connected to MongoDB, database: '{DB_NAME}'.")

def close_mongo_connection():
    """Closes the MongoDB connection."""
    if db_mongo.client:
        #print("Closing MongoDB connection...")
        db_mongo.client.close()