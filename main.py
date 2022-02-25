from src.mongo_util import mongodb_util
from bson.objectid import ObjectId

if __name__ == '__main__':

    # Create the MongoDB util object
    connection_url="mongodb+srv://root:root@myatlascluster.5nfni.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    db_name='test_db'
    col_name='nano_collection'
    logfile='logs/all_logs.log'
    mongo_util_obj = mongodb_util(connection_url,db_name,col_name,logfile)

    # Delete all records
    mongo_util_obj.delete_all_records()

    # Insert all records from csv
    filepath='data/carbon_nanotubes.csv'
    delim_csv=';'
    mongo_util_obj.storecsv_into_db(filepath,delim_csv)

    # Insert one record
    record = {'Chiral indice n': '77', 'Chiral indice m': '100', 'Initial atomic coordinate u': '0,679005', 'Initial atomic coordinate v': '0,701318', 'Initial atomic coordinate w': '0,017033', "Calculated atomic coordinates u'": '0,721039', "Calculated atomic coordinates v'": '0,730232', "Calculated atomic coordinates w'": '0,017014'}
    mongo_util_obj.insert_one_record(record)

    # Insert many records
    list_records = [
                {'Chiral indice n': '88', 'Chiral indice m': '100', 'Initial atomic coordinate u': '0,679005', 'Initial atomic coordinate v': '0,701318', 'Initial atomic coordinate w': '0,017033', "Calculated atomic coordinates u'": '0,721039', "Calculated atomic coordinates v'": '0,730232', "Calculated atomic coordinates w'": '0,017014'},
                {'_id': ObjectId("6217de9e3cc05777bcb9cba7"),'Chiral indice n': '99', 'Chiral indice m': '100', 'Initial atomic coordinate u': '0,679005', 'Initial atomic coordinate v': '0,701318', 'Initial atomic coordinate w': '0,017033', "Calculated atomic coordinates u'": '0,721039', "Calculated atomic coordinates v'": '0,730232', "Calculated atomic coordinates w'": '0,017014'}
    ]
    mongo_util_obj.insert_many_records(list_records)

    # Update one record
    present_data = {'Chiral indice n': '88'}
    new_data = {"$set":{'Chiral indice n': '888'}}
    mongo_util_obj.update_one_record(present_data,new_data)

    # Find one and update
    id = {"_id" : ObjectId("6217de9e3cc05777bcb9cba7")}
    new_data = {"$set":
        {"Chiral indice n": "9999"}
    }
    mongo_util_obj.find_one_record_and_update(id,new_data)

    # Update many records
    present_data = {'Chiral indice n': '77'}
    new_data = {"$set":{'Chiral indice n': '7777777'}}
    mongo_util_obj.update_many_records(present_data,new_data)

    # Delete one record
    query = {'Chiral indice n': '888'}
    mongo_util_obj.delete_one_record(query)

    # Delete many records
    query = {'Chiral indice n': '2'}
    mongo_util_obj.delete_many_records(query)

    # Find one record
    mongo_util_obj.find_one_record()


    #Filter records based on query
    find_query = {"$expr": {"$gt": [{"$toInt" :"$Chiral indice n"} , 77]}}
    mongo_util_obj.filter_records(find_query)

