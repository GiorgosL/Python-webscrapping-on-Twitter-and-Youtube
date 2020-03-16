from pymongo import MongoClient

#Create a class to connect to local host MongoDB and import to other scripts
class mongoConnect():
    def __init__(self,data):
      self.client = MongoClient('localhost', 27017)
      self.db = self.client["DataCollectionG"]
      self.db_collection = self.db[data]