import pymongo
import csv
from src.logger_util import custom_logger

class mongodb_util:

    def __init__(self,connection_url,db_name,col_name,logfile):
        self.logger=custom_logger(logfile).get_logger()
        self.connection(connection_url,db_name,col_name)

    def connection(self,connection_url,db_name,col_name):
        try:
            self.logger.info("Connecting to the MongoDB...")
            self.client = pymongo.MongoClient(connection_url)
            self.db = self.client[db_name]
            self.col = self.db[col_name]
        except Exception:
            self.logger.exception('Exception raised when connecting to MongoDB')
        else:
            self.logger.info("Connection to the MongoDB successful")

    def storecsv_into_db(self,filepath,delim_csv=','):
        try:
            self.logger.info("Storing the csv data into the DB..")
            #Read the csv file and store all the records into a list
            all_records=[]
            with open(filepath, 'r') as f:
                carbon_data=csv.reader(f,delimiter='\n')
                headers=next(carbon_data)[0].split(delim_csv)
                for i,line in enumerate(carbon_data):
                    values=line[0].split(delim_csv)
                    record={}
                    for i,r in enumerate(headers):
                        record[headers[i]]=values[i]
                    all_records.append(record)

            print(all_records[0])
            #Insert into MongoDB database
            #col.insert_many(all_records)
        
        except Exception:
            self.logger.exception('Exception raised while storing data into MongoDB.')
        else:
            self.logger.info("Storing into the MongoDB successful")

    def delete_all_records(self):
        try:
            self.logger.info("Deleting all records from the MongoDB collection")
            # Delete all records
            self.col.delete_many({})

        except Exception:
            self.logger.exception('Exception raised while deleting all records from the MongoDB collection')
        else:
            self.logger.info("Deletion of all records from the MongoDB collection successful")

    def insert_one_record(self,record):
        try:
            self.logger.info("Inserting one record into MongoDB")
            rec = self.col.insert_one(record)
            self.logger.info("Inserted ids are:")
            self.logger.info(f"1. {rec.inserted_id}")

        except Exception:
            self.logger.exception('Exception raised while inserting one record into the MongoDB.')
        else:
            self.logger.info("Insertion of one record into MongoDB successful")
    
    def insert_many_records(self,list_records):
        try:
            self.logger.info("Inserting one record into MongoDB")
            rec = self.col.insert_many(list_records)
            self.logger.info("Inserted ids are:")
            for idx, unique_ids in enumerate(rec.inserted_ids):
                self.logger.info(f"{idx}. {unique_ids}")
        except Exception:
            self.logger.exception('Exception raised while inserting many records into the MongoDB.')
        else:
            self.logger.info("Insertion of many records into MongoDB successful")

    def find_one_record(self):
        try:
            self.logger.info("Finding one record in MongoDB")
            record = self.col.find_one()
            self.logger.info(f"Record found: {record}")
        except Exception:
            self.logger.exception('Exception raised while finding one record from MongoDB.')
        else:
            self.logger.info("Finding one record in MongoDB successful.")
    
    def update_one_record(self,present_data,new_data):
        try:
            self.logger.info("Updating one record in MongoDB")
            self.col.update_one(present_data,new_data)
        except Exception:
            self.logger.exception('Exception raised while updating one record from MongoDB.')
        else:
            self.logger.info("Updating one record in MongoDB successful.")