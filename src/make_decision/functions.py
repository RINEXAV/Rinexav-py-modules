# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 16:20:52 2022

@author: filip
"""
import os
import pandas as pd
import numpy as np 
import datetime
from sklearn.cluster import KMeans, AgglomerativeClustering
import topsis_FG as top
import copras as cop
import sys


class Preprocessing:
    def __init__(self, date, lastDate, sys_bar, freq_done, weights):
        self.date = date
        self.lastDate = lastDate
        self.sys_bar = sys_bar
        self.freq_done = freq_done
        self.weights = weights


    def how_empty(self, file, IGSNetwork):
        '''
        Based on the daily satellite availability file, the function checks 
        whether any signal was received at each station. This check is caused by either 
        a missing RINEX file or an empty RINEX file.
        :param file: dataframe with daily avability of signals
        :param IGSNetwork: dataframe with MGEX stations and their parameters 
        (names, coordinates, priority_level, empty column stats for avability)

        :return: Dataframe with column contains avability of rinex file to this direct file
        '''
        for column in file.columns:
            if file[column].sum() == 0:
                try:
                    IGSNetwork.loc[column,'stats'] = IGSNetwork.loc[column,'stats'] + 1
                except(KeyError):
                    continue     

        return IGSNetwork


    def availability(self, db_func, IGSNetwork, date, lastDate):
        '''
        :param db_func: database management function
        :param IGSNetwork: dataframe with MGEX stations and their parameters
        :param date: start date by which analyst will begin
        :param lastDate: end date by which the analysis will be performed
        
        :return: dataframe with column which are the ranking in each cluster
        '''
        IGSNetwork['stats'] = 0
        i = (lastDate - date).days + 1
        
        while date <= lastDate:
            conditions = {'year': date.year, 'day_of_year': date.timetuple().tm_yday}
            data_availability = db_func.fetch_all('data_availability', **conditions)
            plik = pd.DataFrame(data_availability)
            plik = plik.iloc[:, [2,3,4,5]]
            plik.columns = ['sys_name', 'PRN', 'station_name', 'availability']
            unique_pairs = plik[['sys_name', 'PRN']].drop_duplicates()
            plik = plik.pivot_table(index = ['sys_name', 'PRN'], columns = 'station_name', values = 'availability', dropna = False)
            plik = plik.loc[unique_pairs.set_index(['sys_name', 'PRN']).index]

            IGSNetwork = self.how_empty(plik, IGSNetwork)
            date += datetime.timedelta(days=1)
        IGSNetwork.loc[:,'stats'] = ((i-IGSNetwork.iloc[:,7:8])/i) * 100

        return IGSNetwork


    def process_file(self, db_func, date, lastDate):
        '''
        Main function to generate mean statistics from all avialble canals.
        :param db_func: database management function
        :param date: start date by which analyst will begin
        :param lastDate: end date by which the analysis will be performed

        :return: dataframe only with mean of parameters
        '''
        print("Processing files...")
        snr_mean = pd.DataFrame()
        gaps_mean = pd.DataFrame()
        obs_mean = pd.DataFrame()
        mp_mean = pd.DataFrame()
        
        while date <= lastDate:
        
            conditions = {'year': date.year, 'day_of_year': date.timetuple().tm_yday}
            data_quality = db_func.fetch_all('data_quality', **conditions)
    
            daily_q = pd.DataFrame(data_quality)
            daily_q = daily_q.iloc[:, [2,3,4,5,6,7,8]]
            daily_q.columns = ['sys_name', 'frequency', 'station_name', 'snr', 'obs', 'gaps', 'multipath']
            daily_q.set_index(['sys_name', 'frequency', 'station_name'], inplace=True)
            daily_q = daily_q.unstack(level='station_name')
            daily_q.columns = pd.MultiIndex.from_tuples(daily_q.columns, names=['station_name', 'metric'])
            daily_q.columns = daily_q.columns.swaplevel('metric', 'station_name')
            daily_q = daily_q.sort_index(axis=1)
                        
            snr = daily_q.stack(future_stack = True).T.loc[:, (slice(None), slice(None), "snr")]
            snr_mean = pd.concat([snr_mean, snr], axis=1)

            gaps = daily_q.stack(future_stack = True).T.loc[:, (slice(None), slice(None), "gaps")]
            gaps_mean = pd.concat([gaps_mean, gaps], axis=1)
            
            obs = daily_q.stack(future_stack = True).T.loc[:, (slice(None), slice(None), "obs")]
            obs_mean = pd.concat([obs_mean, obs], axis=1)
            
            mp = daily_q.stack(future_stack = True).T.loc[:, (slice(None), slice(None), "multipath")]
            mp_mean = pd.concat([mp_mean, mp], axis=1)

            date += datetime.timedelta(days=1)
        
        df = pd.concat([snr_mean, obs_mean, gaps_mean, mp_mean], axis=1)

        return df
    

    def dir_to_pick_file(self, directory, date, endpoint):
        '''
        Function to looking for direction to file which will correspond 
        to files from a specific date.
        :param directory: path to folder where script should looking for
        :param date: date of the status file that the script should looking for
        :param endpoint: extension of file (e.g. .csv)
        
        :return: path to status file
        '''
        year = date.strftime('%Y')
        doy = date.strftime('%j')
        list_of_file = os.listdir(directory)
        file = [os.path.join(directory, file) for file in list_of_file 
                if file.endswith(f"{year}_{doy}{endpoint}")][0]

        return file


    def mean_parameters(self, selected_dataset, freq_done):
        '''
        :param selected_dataset: dataframe with only canals selected by user
        :param freq_done: selected canals by user

        :return dataframe with statists to all stations to all selected canals and timeperiod
        '''
        parameters = list(set(selected_dataset.columns.get_level_values(2)))
        full_mean_short = pd.DataFrame()
        full_mean_long = pd.DataFrame()
        for freq in freq_done:
            for par in parameters:
                if selected_dataset.shape[1] == 4*len(freq_done):
                    mean_par = pd.DataFrame(selected_dataset.loc[:, (freq[0], freq[1], par)], columns=[(freq[0], freq[1], par)])
                    full_mean_short = pd.concat([full_mean_short, mean_par], axis=1, join='outer')

                else:
                    mean_par = pd.DataFrame(selected_dataset.loc[:, (freq[0], freq[1], par)].T.mean(), columns=[(freq[0], freq[1], par)])
                    full_mean_long = pd.concat([full_mean_long, mean_par], axis=1, join='outer')
            
        if selected_dataset.shape[1] == 4*len(freq_done):
            return full_mean_short
        else:
           return full_mean_long 


    def traverse_columns(self, mean_parameters, freq_done):
        '''
        Function to traverse columns when the first 4 columns is empty, it gives algorithm easier processing.
        :param mean_parameters: mean parameters to selected canals
        :param freq_done: it's only for number of selected canals

        :return: traversed mean parameters with failed column names but it's only halfproduct
        '''
        for x,y in zip([0,1,2,3]*(len(freq_done)), range((4*len(freq_done)-4))):
            mean_parameters.iloc[:, x] = mean_parameters.iloc[:, x].fillna(mean_parameters.iloc[:, y+4])
            
        return mean_parameters


    def select_thebest(self, mean_parameters_trav, freq_done):
        '''
        Function to select canal with the largest number of observations.
        :param mean_parameters_trav: traversed mean parameters
        :param freq_done: it's only for number of selected canals

        :return: mean parametrs for canal which have the largest number of observations
        '''
        n=4
        for freq in range(len(freq_done)-1):
            hahs = mean_parameters_trav.iloc[:, n:n+4].loc[mean_parameters_trav.loc[:, [(freq_done[0][0], freq_done[0][1], 'obs')]].values < mean_parameters_trav.loc[:, [(freq_done[1+freq][0], freq_done[1+freq][1], 'obs')]].values]

            mean_parameters_trav.iloc[:, :4].loc[hahs.index] = hahs
            n+=4
            
        to_go = mean_parameters_trav.iloc[:,:4]
        columns = to_go.columns.to_list()
        to_go.columns = [columns[0][2], columns[1][2], columns[2][2], columns[3][2]]
        to_go = to_go[['obs', 'snr', 'gaps', 'multipath']]
        return to_go


class Selecting:
    def __init__(self, ileprocent, clustering_method, MDCA_method, num_points):
        self.ileprocent = ileprocent
        self.clustering_method = clustering_method
        self.MDCA_method = MDCA_method
        self.num_points = num_points
        

    def dividing_stations(self, IGSNetwork, num_points):
        '''
        Implementation of the KMeans clustering algorithm.
        :param IGSNetwork: dataframe with coordinates and statistics only coordinates XYZ are used
        :param num_points: number of center points of clusters e.g. if you choose 100 the algorithm divides all available stations to 100 clusters

        :return: dataframe with labels which mean assigment to cluster
        '''

        algorytm = KMeans(n_clusters=num_points, random_state=0)
        
        labels = algorytm.fit(np.array(IGSNetwork.iloc[:,:3])).labels_
        labels = pd.DataFrame(labels, index=IGSNetwork.index,columns=['segment'])
        IGSNetwork = pd.concat([IGSNetwork, labels], axis=1)
        
        return IGSNetwork


    def MDCA(self, IGSNetwork, weights, num_points):
        '''
        A function that creates a ranking based on the MDCA TOPSIS method.
        :param IGSNetwork: dataframe with coordinates, statistics and number of cluster to export in the end full dataframe

        :return: dataframe with column which are the ranking in each cluster
        '''
        mat = pd.DataFrame()
        crit = [1, 1, -1, -1]
        criterias = np.array([-1])
        
        crit2 = np.array(['max', 'max', 'min', 'min'])
        criterias2 = np.array(['min'])

        criterias = np.hstack((criterias,crit))
        criterias2 = np.hstack((criterias2,crit2))

        for segment in range(0,num_points):
            seg = IGSNetwork[IGSNetwork.loc[:,"segment"]==segment].drop("stats", axis=1)
            evaluation_matrix = np.array(seg.iloc[:,6:-1])
            
            rank = top.topsis(evaluation_matrix, weights, criterias)
            df = pd.DataFrame(rank, index=seg.index, columns=["TOPSIS"])
            
            mat = pd.concat([mat, df])
        IGSNetwork = pd.concat([IGSNetwork, mat], axis=1)

        return IGSNetwork