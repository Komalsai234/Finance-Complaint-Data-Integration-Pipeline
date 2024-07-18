import pymongo
from constant import *

class Mongo_Client():
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_DB_URI)
        self.db = self.client[MONGO_DB_DATABASE]
        self.collection = self.db[MONGO_DB_COLLECTION]

    def get_collection(self):
        return self.collection

    def close(self):
        self.client.close()