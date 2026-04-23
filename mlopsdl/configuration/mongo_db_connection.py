import sys

from mlopsdl.exception import MLOpsException
from mlopsdl.logger import logging

import os
from dotenv import load_dotenv
from mlopsdl.constants import DATABASE_NAME, MONGODB_URL_KEY
import pymongo
import certifi

ca = certifi.where()
load_dotenv()
class MongoDBClient:

    client = None

    def __init__(self, database_name = DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv("MONGODB_URL")
                if mongo_db_url is None:
                    raise MLOpsException(f"MongoDB URL not found in environment variables with key: {"MONGODB_URL"}", sys)
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info(f"Connected to MongoDB database: {database_name}")
        except Exception as e:
            raise MLOpsException(e, sys)