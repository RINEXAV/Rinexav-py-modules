import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


#--------------------------------DOWNLOAD CSV FROM FTP.AIUB.UNIBE.CH-------------------------------#

def download_satellites_info_file(response_url, file_path):
    """
    Downloads a file with information about GNSS satellites.

    :param response_url: Link to download the file.
    :param file_path: Path to save the file.

    :return: Downloaded file saved to disk.
    """
    response = requests.get(response_url)
    if response.status_code in range(200, 300):
        with open(os.path.join(file_path, "SATELLIT_I20.SAT"), "wb") as file:
            file.write(response.content)
    elif response.status_code in range(400, 500):
        print("Client Error: File download failed")
    elif response.status_code in range(500, 600):
        print("Server Error: File download failed")
    else:
        print("Another Error: File download failed")
        
#-------------------------------------FIND CSV FILES TO UDPATE-------------------------------------#

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

#-----------------------------------UPDATE SATELLITES PRN LIST-------------------------------------#

def update_satellites(file_path):
    """
    Updates the list of GNSS satellite PRN numbers contained in the .txt file.

    :param file_path: Path to .txt file.

    :return: Updated .txt file.
    """
    file_name = "sat.txt"
    updated_file = find_files_to_update(file_path, file_name)
    downloaded_file = os.path.join(file_path, "SATELLIT_I20.SAT")
    
    with open(downloaded_file, 'r') as file:
        content = file.readlines()

    column_names = ["PRN", "SVN", "BLOCK_NO", "COSPAR_ID", "ATTITUDE_FLAG",
                    "START_YEAR", "START_MONTH", "START_DAY", "START_HOUR", "START_MINUTE", "START_SECOND",
                    "END_YEAR", "END_MONTH", "END_DAY", "END_HOUR", "END_MINUTE", "END_SECOND",
                    "MASS", "AREA_MASS", "RADIATION_PRESSURE_C0", "AIR_DRAG_MODEL", "AIR_DRAG_C0", "PLN_SLT"]

    data_rows = []
    for line in content:
        if re.match(r'^\s*\d+\s+\d+', line):
            split_line = re.split(r'\s+', line.strip())
            data_rows.append(split_line)

    df = pd.DataFrame(data_rows, columns = column_names)        
    df.replace('', pd.NA, inplace = True)
    df['END_YEAR'] = pd.to_numeric(df['END_YEAR'], errors = 'coerce')
    df['PRN'] = df['PRN'].astype(str)
    df['PRN'] = df['PRN'].str.zfill(3)

    filtered_data = df[df['END_YEAR'].isna()]
    found_prns = filtered_data['PRN'].tolist()

    system_names_numeric = {'G': 0, 'R': 1, 'E': 2, 'C': 4, 'J': 5}

    prn_letters = []
    prn_numbers = []
    for prn in found_prns:
        numeric_sys_name = prn[0]
        for letter, number in system_names_numeric.items():
            if numeric_sys_name == str(number):
                prn_letters.append(letter)
                prn_numbers.append(letter + prn[1::])
                break
    
    prn_letters_df = pd.DataFrame(prn_letters)
    prn_numbers_df = pd.DataFrame(prn_numbers)
    sys_sat_list = pd.concat([prn_letters_df, prn_numbers_df], axis = 1, ignore_index = True)

    irnss_url = "https://www.igs.org/mgex/constellations/"
    response = requests.get(irnss_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    irnss_section = soup.find(id = 'navic')
    irnss_table = irnss_section.find('table', class_ = 'mgextable')
    
    irnss_prn_list = []
    for row in irnss_table.find_all('tr')[1:]:
        columns = row.find_all('td')
        prn = columns[4].text.strip()
        if re.compile(r'^I\d{2}$').match(prn):
            irnss_prn_list.append(prn)

    irnss_prn_letters_df = pd.DataFrame(prn[0] for prn in irnss_prn_list)
    irnss_prn_numbers_df = pd.DataFrame(irnss_prn_list)
    irnss_sat_list = pd.concat([irnss_prn_letters_df, irnss_prn_numbers_df], axis = 1, ignore_index = True)

    sat = pd.concat([sys_sat_list, irnss_sat_list], axis = 0, ignore_index = True)

    for file in updated_file:
        sat.to_csv(file, index = False, header = None)

def main():
    path = os.path.dirname(os.path.realpath(os.path.dirname(__file__)))
    url = "http://ftp.aiub.unibe.ch/BSWUSER54/CONFIG/SATELLIT_I20.SAT"

    try:
        download_satellites_info_file(url, path)
        update_satellites(path)
        print("The list of satellite PRN numbers has been updated")

    except FileNotFoundError:
        pass

    finally:
        
        try:
            os.remove(os.path.join(path, "SATELLIT_I20.SAT"))
            
        except FileNotFoundError:
            pass
            
if __name__ == "__main__":
    main()       