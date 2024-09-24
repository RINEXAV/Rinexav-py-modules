import pandas as pd
import requests
import os
from database_manager_SQLite import DatabaseManager
import sqlite3


def download_igs_network_file(response_url, file_path):
    """
    Downloads a file with information about IGS stations.

    :param response_url: Link to download the file.
    :param file_path: Path to save the file.

    :return: Downloaded file saved to disk.
    """
    response = requests.get(response_url)
    if response.status_code in range(200, 300):
        with open(os.path.join(file_path, "IGS_stations.csv"), "wb") as file:
            file.write(response.content)
    elif response.status_code in range(400, 500):
        print("Client Error: File download failed")
    elif response.status_code in range(500, 600):
        print("Server Error: File download failed")
    else:
        print("Another Error: File download failed")


def select_multi_gnss(file_path):
    """
    Selects from the IGS stations file only those stations that use Multi-GNSS.

    :param file_path: Path to the IGS stations file.

    :return: CSV file with selected IGS stations.
    """
    igs_stations = os.path.join(file_path, "IGS_stations.csv")
    df = pd.read_csv(igs_stations)
    df = df[df["Networks"].str.contains("IGS Multi-GNSS", na = False, case = False)]
    df.to_csv(igs_stations, index = False)


def add_priority_levels(file_path):
    igs_stations = os.path.join(file_path, "IGS_stations.csv")
    priority_levels = os.path.join(file_path, "priority_levels.txt")

    df1 = pd.read_csv(igs_stations)
    df2 = pd.read_csv(priority_levels, sep = r"\s+", skiprows = 119)

    df1["short_names"] = df1["Site Name"].str[0:4]
    merged_df = pd.merge(df1, df2[["code", "pr"]], left_on = "short_names", right_on = "code", how = "left")
    merged_df.fillna({"pr": 11}, inplace = True)
    merged_df.drop(columns = ["short_names"], inplace = True)
    merged_df.to_csv(os.path.join(file_path, "IGS_stations.csv"), index = False)


def main():
    path = os.path.dirname(os.path.abspath(__file__))
    url = "https://network.igs.org/api/public/stations.csv?draw=1&length=767&current=true&ordering=name&fields%5B%5D=name&fields%5B%5D=country&fields%5B%5D=receiver_type&fields%5B%5D=antenna_type&fields%5B%5D=radome_type&fields%5B%5D=satellite_system&fields%5B%5D=latitude&fields%5B%5D=longitude&fields%5B%5D=elevation&fields%5B%5D=x&fields%5B%5D=y&fields%5B%5D=z&fields%5B%5D=antcal&fields%5B%5D=network&fields%5B%5D=data_center&fields%5B%5D=domes_number&fields%5B%5D=frequency_standard&fields%5B%5D=agency&fields%5B%5D=last_data&start=0"

    db_manager = DatabaseManager(database_name = os.path.join(path, '../database/rinexav_db.db'))

    try:
        download_igs_network_file(url, path)
        select_multi_gnss(path)
        add_priority_levels(path)

        igs_stations = os.path.join(path, "IGS_stations.csv")
        df = pd.read_csv(igs_stations)
    
        table = "stations"
        db_manager.create_table(f"DELETE FROM {table}")
    
        for index, row in df.iterrows():
            values = [index + 1, row["Site Name"], row["X (m)"], row["Y (m)"], row["Z (m)"], row["Latitude"], row["Longitude"], row["Height (m)"], row["Receiver"], row["pr"]] 
        
            try:
                db_manager.insert(table, *values)

            except sqlite3.IntegrityError:
                db_manager.connection.rollback()
                            
    except FileNotFoundError:
        pass

    finally:       
        try:
            print("The list of IGS stations and their coordinates have been updated")
            os.remove(os.path.join(path, "IGS_stations.csv"))

        except FileNotFoundError:
            pass
     

if __name__ == "__main__":
    main()