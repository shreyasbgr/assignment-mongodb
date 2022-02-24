import pymongo
import csv
from logger_util import custom_logger

class mongodb_util:

    def __init__(self,connection_url,db_name,col_name,logfile):
        self.logger=custom_logger(logfile).get_logger()
        self.connection(connection_url,db_name,col_name)

    def connection(self,connection_url,db_name,col_name):
        try:
            self.logger.info("Connecting to the MongoDB...")
            client = pymongo.MongoClient(connection_url)
            self.db = client[db_name]
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

            #Insert into MongoDB database
            #col.insert_many(all_records)
            # Delete all records
            self.col.delete_many({})
        
        except Exception:
            self.logger.exception('Exception raised while storing data into MongoDB.')
        else:
            self.logger.info("Storing into the MongoDB successful")

if __name__ == '__main__':
    connection_url="mongodb+srv://root:root@myatlascluster.5nfni.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    db_name='test_db'
    col_name='nano_collection'
    logfile='logs/all_logs.log'
    mongo_util_obj = mongodb_util(connection_url,db_name,col_name,logfile)

    filepath='data/carbon_nanotubes.csv'
    delim_csv=';'
    mongo_util_obj.storecsv_into_db(filepath,delim_csv)