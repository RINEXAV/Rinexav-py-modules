import os
import pandas as pd
import datetime
from functions import Preprocessing as pre
from functions import Selecting as sel
import yaml

def main():

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

    dirname = os.path.dirname(os.path.abspath("__file__"))
    dir_to_station_list = os.path.join(dirname, "station_coords.csv")
    IGSNetwork = pd.read_csv(dir_to_station_list).set_index("#StationName")

    IGSNetwork = preprocessing.availability(IGSNetwork, "rv3_stat", date, lastDate)

    df = preprocessing.process_file("rv3_stat", date, lastDate)
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
        
        IGSNetwork = selecting.dividing_stations(IGSNetwork, clustering_method, num_points)
        IGSNetwork = selecting.MDCA(IGSNetwork, MDCA_method, weights, num_points)
        wybor = IGSNetwork[IGSNetwork.loc[:,MDCA_method]==1].iloc[:,3:]
        IGSNetwork = IGSNetwork.iloc[:,3:]
        
        print(IGSNetwork)
        print(wybor)
        wybor.to_csv("selected.csv")
        IGSNetwork.to_csv("all_stations.csv")
    
if __name__ == "__main__":
    main()
