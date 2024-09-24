import os
import requests
import pandas as pd


#---------------------------------DOWNLOAD CSV FROM NETWORK.IGS.ORG--------------------------------#

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
        
#------------------------------SELECTION OF STATIONS USING MULTI-GNSS------------------------------#

def select_multi_gnss(file_path):
    """
    Selects from the IGS stations file only those stations that use Multi-GNSS.

    :param file_path: Path to the IGS stations file.

    :return: CSV file with selected IGS stations.
    """
    downloaded_file = os.path.join(file_path, "IGS_stations.csv")
    df = pd.read_csv(downloaded_file)
    df = df[df["Networks"].str.contains("IGS Multi-GNSS", na = False, case = False)]
    df.to_csv(downloaded_file, index = False)

#-----------------------------------FIND IGS CSV FILES TO UDPATE-----------------------------------#

def find_files_to_update(file_path, file_name):
    """
    Searches the folder for files to update.

    :param file_path: Path to the folder in which to search for files.
    :param file_name: The name of the file to search for.

    :return found_files: List of paths to found files. 
    """
    found_files = []
    for root, dir, files in os.walk(file_path):
        if file_name in files:
            found_files.append(os.path.join(root, file_name))
    return found_files

#---------------------------------UPDATE IGS NETWORK STATIONS LIST---------------------------------#

def update_igs_network_list(file_path):
    """
    Updating .csv file with IGS Multi-GNSS station names.

    :param file_path: Path to .csv file.

    :return: Updated .csv file.    
    """
    file_name = "IGSNetwork_new.csv"
    updated_file = find_files_to_update(file_path, file_name)
    downloaded_file = os.path.join(file_path, "IGS_stations.csv")
    
    for file in updated_file:
        df = pd.read_csv(downloaded_file, usecols = ["Site Name"])
        df.columns = ["StationName"]
        df.sort_values(by = "StationName")
        df.to_csv(file, index = False)

#-------------------------------UPDATE IGS NETWORK COORDINATES LIST--------------------------------#

def update_igs_network_coords(file_path):
    """
    Updating .csv file with IGS Multi-GNSS station coordinates.

    :param file_path: Path to .csv file.

    :return: Updated .csv file.    
    """
    file_name = "MGEX_wsp.csv"
    updated_file = find_files_to_update(file_path, file_name)
    downloaded_file = os.path.join(file_path, "IGS_stations.csv")

    for file in updated_file:
        df = pd.read_csv(downloaded_file, usecols = ["Site Name", "X (m)", "Y (m)", "Z (m)", "Latitude", "Longitude", "Height (m)", "Receiver"])
        df = df.iloc[:, [0, 5, 6, 7, 2, 3, 4, 1]]
        df.columns = ["#StationName", "X", "Y", "Z", "Latitude", "Longitude", "Height", "ReceiverName"]
        df.sort_values(by = "#StationName")
        df.insert(loc = 0, column = "id", value = [i for i in range(1, len(df) + 1)])
        df.to_csv(file, index = False)

def main():
    path = os.path.dirname(os.path.realpath(os.path.dirname(__file__)))
    url = "https://network.igs.org/api/public/stations.csv?draw=1&length=767&current=true&ordering=name&fields%5B%5D=name&fields%5B%5D=country&fields%5B%5D=receiver_type&fields%5B%5D=antenna_type&fields%5B%5D=radome_type&fields%5B%5D=satellite_system&fields%5B%5D=latitude&fields%5B%5D=longitude&fields%5B%5D=elevation&fields%5B%5D=x&fields%5B%5D=y&fields%5B%5D=z&fields%5B%5D=antcal&fields%5B%5D=network&fields%5B%5D=data_center&fields%5B%5D=domes_number&fields%5B%5D=frequency_standard&fields%5B%5D=agency&fields%5B%5D=last_data&start=0"

    try:
        download_igs_network_file(url, path)
        select_multi_gnss(path)
        update_igs_network_list(path)
        update_igs_network_coords(path)
        print("The list of IGS stations and their coordinates have been updated")

    except FileNotFoundError:
        pass

    finally:
        
        try:
            os.remove(os.path.join(path, "IGS_stations.csv"))

        except FileNotFoundError:
            pass
            
if __name__ == "__main__":
    main()