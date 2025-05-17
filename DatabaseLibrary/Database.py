from abc import ABC,abstractmethod
import pymongo
from bson import ObjectId

class DatabaseAbstract(ABC):
    @abstractmethod
    def __init__(self):
        pass
    @abstractmethod
    def connect(self) -> bool:
        pass
    @abstractmethod
    def create(self) -> tuple:
        pass
    @abstractmethod
    def read(self) -> list:
        pass
    @abstractmethod
    def update(self) -> tuple:
        pass
    @abstractmethod
    def delete(self) -> tuple:
        pass

class MongoDatabase(DatabaseAbstract):
    def __init__(self):
        super().__init__()
    def connect(self,name_db:str,name_collections:list) -> bool:
        super().connect()
        try:
            self.__clientDB = pymongo.MongoClient('mongodb://localhost:27017/')
            self.__name_db = name_db
            self.__name_collections = name_collections 
            self.__collectionsDB = {}
            self.__clientDB.admin.command("ping")
            self.__database = self.__clientDB[self.__name_db]
            for name in self.__name_collections:
                self.__collectionsDB[name] = self.__database[name]
            return True
        except Exception as e:
            print(e)
        return False
    def create(self,collection_name:str,document:dict):
        super().create()
        try:
            res = self.__collectionsDB[collection_name].insert_one(document)
            return res.inserted_id
        except Exception as e:
            return None 
    def read(self,collection_name:str,query:dict) -> list:
        super().read()
        try:
            return list(self.__collectionsDB[collection_name].find(query))
        except Exception as e:
            return None
    def update(self,collection_name:str,value:dict,query:dict) -> tuple:
        super().update()
        try:
            update = {"$set":value}
            res = self.__collectionsDB[collection_name].update_many(query,update)
            return res.modified_count
        except Exception as e:
            return None
    def delete(self,collection_name:str,query:dict):
        super().delete()
        try:
            res = self.__collectionsDB[collection_name].delete_many(query)
            return res.deleted_count
        except Exception as e:
            return None

class DatabaseType:
    MONGO_DB = 1

class DatabaseFactory:
    def CreatDatabase(type:DatabaseType) -> DatabaseAbstract:
        db = None
        if type == DatabaseType.MONGO_DB:
            db = MongoDatabase()
        return db
