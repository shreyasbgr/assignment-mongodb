from src.mongo_util import mongodb_util

if __name__ == '__main__':

    # Create the MongoDB util object
    connection_url="mongodb+srv://root:root@myatlascluster.5nfni.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    db_name='test_db'
    col_name='nano_collection'
    logfile='logs/all_logs.log'
    mongo_util_obj = mongodb_util(connection_url,db_name,col_name,logfile)

    # Insert all records from csv
    """filepath='data/carbon_nanotubes.csv'
    delim_csv=';'
    mongo_util_obj.storecsv_into_db(filepath,delim_csv)

    # Insert one record
    record = {'Chiral indice n': '77', 'Chiral indice m': '100', 'Initial atomic coordinate u': '0,679005', 'Initial atomic coordinate v': '0,701318', 'Initial atomic coordinate w': '0,017033', "Calculated atomic coordinates u'": '0,721039', "Calculated atomic coordinates v'": '0,730232', "Calculated atomic coordinates w'": '0,017014'}
    mongo_util_obj.insert_one_record(record)

    # Insert many records
    list_records = [
                {'Chiral indice n': '88', 'Chiral indice m': '100', 'Initial atomic coordinate u': '0,679005', 'Initial atomic coordinate v': '0,701318', 'Initial atomic coordinate w': '0,017033', "Calculated atomic coordinates u'": '0,721039', "Calculated atomic coordinates v'": '0,730232', "Calculated atomic coordinates w'": '0,017014'},
                {'Chiral indice n': '99', 'Chiral indice m': '100', 'Initial atomic coordinate u': '0,679005', 'Initial atomic coordinate v': '0,701318', 'Initial atomic coordinate w': '0,017033', "Calculated atomic coordinates u'": '0,721039', "Calculated atomic coordinates v'": '0,730232', "Calculated atomic coordinates w'": '0,017014'}
    ]
    mongo_util_obj.insert_many_records(list_records)"""

    # Find one record
    mongo_util_obj.find_one_record()