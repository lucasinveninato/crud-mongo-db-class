from mongo_db_utils import MongoDBConnector


db_name = 'your_db_name'
collection = 'your_collection'

#This example is for local databases. If you're using a database on a provider, you'll need to provide the correct host and credentials as parameters to the class. 
with MongoDBConnector(host='localhost') as connection:
    connection.create_document(db_name=db_name,collection_name=collection, document={"YOUR_JSON"})
    connection.delete_documents(db_name=db_name,collection_name=collection,query={"YOUR_QUERY"})
    documents = connection.read_documents(db_name=db_name,collection_name=collection)
    print(documents)
    connection.update_documents(db_name=db_name,collection_name=collection,query={"YOUR_QUERY"},update_data={"$set":{"YOUR_UPDATE_DATA"}})

