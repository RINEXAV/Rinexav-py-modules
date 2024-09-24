import os
import re
import time
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
    filename_pattern = r"^\d{4}_\d{3}_q\.csv$"
    
    file_list = find_files(pattern = filename_pattern, path = file_path)
    
    db_manager = DatabaseManager(database_name = 'database/rinexav_db.db')
    
    now = datetime.datetime.now()
    print("Processing...")
    for file in file_list:
        load_file = pd.read_csv(file, header=None)
        
        header_row1 = load_file.iloc[0]
        header_row2 = load_file.iloc[1]
        data_rows = load_file.iloc[2:]

        station_count = (len(load_file.columns) - 2) // 4

        for index, row in data_rows.iterrows():
            sys_name = row[0]
            frequency = row[1]

            for i in range(station_count):
                station_name = header_row1[2 + i * 4]
                snr = row[2 + i * 4]
                obs = row[3 + i * 4]
                gaps = row[4 + i * 4]
                multipath = row[5 + i * 4]

                table = 'data_quality'
                values = [file[-14:-10], file[-9:-6], sys_name, frequency, station_name, float(snr), float(obs), float(gaps), float(multipath)]

                try:
                    db_manager.insert(table, *values)
                except sqlite3.IntegrityError:
                    db_manager.connection.rollback()
                finally:
                    now2 = datetime.datetime.now()
                    delta = int((now2 - now).total_seconds())
                    timer(station_name, file[-14:-10], file[-9:-6], delta)

if __name__ == "__main__":
    main()