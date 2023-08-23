import datetime
from pymongo import MongoClient

class MongoDBConnector:
    """
    A Python class for connecting to a MongoDB database and performing basic CRUD operations.

    Parameters:
    - host (str): The hostname or IP address of the MongoDB server.
    - port (int, optional): The port number for the MongoDB server (default is 27017).
    - username (str, optional): The username for authentication (default is None).
    - password (str, optional): The password for authentication (default is None).
    """
    def __init__(self, host:str, port=27017, username=None, password=None):
        """
        Initializes a MongoDBConnector object with the specified connection parameters.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None


    def __enter__(self):
        """
        Enters a context where the MongoDB connection is established. Returns the MongoDBConnector object.
        """
        return self.connect()


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exits the context and closes the MongoDB connection.
        """
        self.client = None

    def connect(self):
        """
        Establishes a connection to the MongoDB server using the provided parameters.

        Returns:
            - MongoClient object: A connection to the MongoDB server.
            - ConnectionError: If the connection to the server fails.
        """
        try:
            if self.username and self.password:
                connection_string = f'mongodb+srv://{self.username}:{self.password}@{self.host}/?retryWrites=true&w=majority'
            else:
                connection_string = f"mongodb://{self.host}:{self.port}"

            self.client = MongoClient(connection_string)
            return self
        
        except ConnectionError as e:
            return e
        

    def create_document(self, db_name:str, collection_name:str, document:dict):
        """
        Inserts a new document into a specified database and collection.

        Parameters:
            - db_name (str): The name of the target database.
            - collection_name (str): The name of the target collection within the database.
            - document (dict): The document to be inserted.

        Returns:
            - str: The unique ID of the inserted document.

        Raises:
            - Exception: If the insertion operation encounters an error.
        """
        db = self.client[db_name]
        collection = db[collection_name]
        try:
            document["created_at"] = datetime.datetime.utcnow()
            document["updated_at"] = datetime.datetime.utcnow()
            result = collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            raise e


    def read_documents(self, db_name:str, collection_name:str, query:dict=None, projection:dict=None):
        """
        Retrieves documents from a specified database and collection based on a query.

        Parameters:
            - db_name (str): The name of the target database.
            - collection_name (str): The name of the target collection within the database.
            - query (dict, optional): A query filter for document selection (default is None).
            - projection (dict, optional): A projection for including/excluding fields (default is None).

        Returns:
            - list: A list of documents matching the query.

        Raises:
            - Exception: If the read operation encounters an error.
        """
        db = self.client[db_name]
        collection = db[collection_name]
        try:
            cursor = collection.find(query, projection)
            documents = [doc for doc in cursor]
            return documents
        except Exception as e:
            raise e


    def update_documents(self, db_name:dict, collection_name:dict, query:dict, update_data:dict):
        """
        Updates documents in a specified database and collection based on a query and update data.

        Parameters:
            - db_name (str): The name of the target database.
            - collection_name (str): The name of the target collection within the database.
            - query (dict): A query filter for document selection.
            - update_data (dict): Data to be updated in matching documents.

        Returns:
            - int: The number of documents updated.

        Raises:
            - Exception: If the update operation encounters an error.
        """
        db = self.client[db_name]
        collection = db[collection_name]
        try:
            update_data["$set"]["updated_at"] = datetime.datetime.utcnow()
            
            result = collection.update_one(query, update_data)
            return result.modified_count
        except Exception as e:
            raise e


    def delete_documents(self, db_name:str, collection_name:str, query:dict):
        """
        Deletes documents from a specified database and collection based on a query.

        Parameters:
            - db_name (str): The name of the target database.
            - collection_name (str): The name of the target collection within the database.
            - query (dict): A query filter for document selection.

        Returns:
            - int: The number of documents deleted.

        Raises:
            - Exception: If the deletion operation encounters an error.
        """
        db = self.client[db_name]
        collection = db[collection_name]
        try:
            result = collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            raise e