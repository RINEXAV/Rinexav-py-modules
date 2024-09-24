import os
import pandas as pd
import datetime
from functions import Preprocessing as pre
from functions import Selecting as sel
import yaml
import sys
import subprocess
import time
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
src_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
modules_path = os.path.join(src_path, 'modules')
sys.path.append(modules_path)
from database_manager_SQLite import DatabaseManager


def update_stations_and_satellites_data(update_IGS_Network_file_path, update_Satellite_list_file_path):
    '''
    Updates data on IGS stations and satellite PRN numbers in the database.
    :param update_IGS_Network_file_path: path to the script responsible for updating the IGS station
    :param update_Satellite_list_file_path: path to the script responsible for updating satellite PRN numbers
    '''
    try:
        IGS_Network_list_update = subprocess.run(['python', update_IGS_Network_file_path], check = True, capture_output = True, text = True)
        Satellite_list_update = subprocess.run(['python', update_Satellite_list_file_path], check = True, capture_output = True, text = True)
        print(IGS_Network_list_update.stdout)
        print(Satellite_list_update.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr}")


def main():
    update_data = True
    
    update_IGS_Network = os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules/update_IGS_Network_DATABASE.py'))
    update_Satellite_list = os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules/update_Satellite_list_DATABASE.py'))
    
    if update_data:
        update_stations_and_satellites_data(update_IGS_Network, update_Satellite_list)  

    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)
        
    freq_done = [tuple(map(str, x.split(','))) for x in config['freq_done']]
    
    date = datetime.date(config['start_date']['year'],
                         config['start_date']['month'],
                         config['start_date']['day'])
    
    lastDate = datetime.date(config['lastDate']['year'],
                             config['lastDate']['month'],
                             config['lastDate']['day'])
    num_points = config['num_points']
    ileprocent = config['ileprocent']
    MDCA_method = config['MDCA_method']
    clustering_method = config['clustering_method']
    weights = config['weights']
    sys_bar = config['sys_bar']

    if date <= lastDate:
        ileprocent = ileprocent
        clustering_method = clustering_method
        MDCA_method = MDCA_method
        num_points = num_points
    else:
        print("ENTER THE RELEVANT DATE RANGE")
        
    preprocessing = pre(date, lastDate, sys_bar, freq_done, weights)
    selecting = sel(ileprocent, clustering_method, MDCA_method, num_points)
    
    db_manager = DatabaseManager(database_name = '../database/rinexav_db.db')
    file = db_manager.fetch_all('stations', **{'1': 1})
    IGSNetwork = pd.DataFrame(file)
    IGSNetwork = IGSNetwork.iloc[:, [1, 2, 3, 4, 5, 6, 7, 9]]
    IGSNetwork.columns = ['#StationName', 'X', 'Y', 'Z', 'Latitude', 'Longitude', 'Height', 'pr_level']
    IGSNetwork = IGSNetwork.set_index('#StationName')
    
    IGSNetwork = preprocessing.availability(db_manager, IGSNetwork, date, lastDate)

    df = preprocessing.process_file(db_manager, date, lastDate)
    freq = df.loc[:, (sys_bar, slice(None), slice(None))].columns.to_list()
    freq = [tuple(list(chanel)[:2]) for chanel in freq]
    freq = sorted(list(set(freq)))
    selected_dataset = pd.DataFrame()
    for x in freq_done:
        out = df.loc[:, (x[0], x[1], slice(None))]
        selected_dataset = pd.concat([selected_dataset, out], axis=1, join='outer')
    
    selected_dataset = selected_dataset.dropna(how='all')  # wszystkie dni
    mean_parameters = preprocessing.mean_parameters(selected_dataset, freq_done) #usrednione
    IGSNetwork = pd.concat([IGSNetwork, mean_parameters], axis=1, join='inner')
    IGSNetwork_to_check = pd.concat([IGSNetwork, mean_parameters], axis=1, join='outer')
    
    check = mean_parameters.shape[0] == IGSNetwork.shape[0]
    if check == False:
        los = IGSNetwork_to_check[IGSNetwork_to_check['X'].isna()].index.to_list()
        print(f"""Stations {los} need to complete the information in the station_coords.csv file. \nPlease complete this information according to the header at the end of the file.""")
    
    else:
        mean_parameters_trav = preprocessing.traverse_columns(mean_parameters, freq_done)
        mean_parameters_ready = preprocessing.select_thebest(mean_parameters_trav, freq_done)
        IGSNetwork = pd.concat([IGSNetwork.iloc[:,:8], mean_parameters_ready], axis=1)
        IGSNetwork = IGSNetwork[IGSNetwork.loc[:,"stats"]>=ileprocent]
        
        IGSNetwork = selecting.dividing_stations(IGSNetwork, num_points)
        IGSNetwork = selecting.MDCA(IGSNetwork, weights, num_points)
        wybor = IGSNetwork[IGSNetwork.loc[:,MDCA_method]==1].iloc[:,3:]
        IGSNetwork = IGSNetwork.iloc[:,3:]
        
        print(IGSNetwork)
        print(wybor)
        wybor.to_csv("selected.csv")
        IGSNetwork.to_csv("all_stations.csv")
    
if __name__ == "__main__":
    main()
