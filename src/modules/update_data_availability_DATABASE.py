import os
import re
import datetime
import pandas as pd
from modules.database_manager_SQLite import DatabaseManager
import sqlite3


def find_files(pattern, path):
    result = []
    regex = re.compile(pattern)
    for root, dir, files in os.walk(path):
        for filename in files:
            if regex.match(filename):
                result.append(os.path.join(root, filename))
    return result


def timer(station, year, doy, delta):
    hours = delta // 3600
    minutes = (delta % 3600) // 60
    seconds = delta % 60
    return print(f"\rStation name: {station}, YEAR:{year} | DOY:{doy}, \
                 Duration: {hours}:{str(minutes).zfill(2)}:{str(seconds).zfill(2)}", end="\r")


def main():
    src_path = os.path.dirname(os.path.realpath(os.path.dirname(__file__)))
    file_path = os.path.join(src_path, 'status_file_automatic/download_data')
    filename_pattern = r"^\d{4}_\d{3}\.csv$"
    
    file_list = find_files(pattern = filename_pattern, path = file_path)
    
    db_manager = DatabaseManager(database_name = 'database/rinexav_db.db')
    
    now = datetime.datetime.now()
    print("Processing...")
    for file in file_list: 
        load_file = pd.read_csv(file)
        
        for index, row in load_file.iterrows():
            sys_name = row['sys_name']
            prn = row['PRN']
        
            for column in load_file.columns[2:]:
                station_name = column
                availability = row[column]
                try:
                    availability = int(availability)
                except:
                    availability = None
                
                table = 'data_availability'
                values = [file[-12:-8], file[-7:-4], sys_name, prn, station_name, availability]
                
                try:
                    db_manager.insert(table, *values)
                except sqlite3.IntegrityError:
                    db_manager.connection.rollback()
                finally:
                    now2 = datetime.datetime.now()
                    delta = int((now2 - now).total_seconds())
                    timer(station_name, file[-12:-8], file[-7:-4], delta)

if __name__ == "__main__":
    main()