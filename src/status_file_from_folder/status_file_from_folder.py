# -*- coding: utf-8 -*-

from xmlrpc.client import boolean
import pandas as pd
import numpy as np
import shutil
import os 
from ftplib import FTP_TLS
import datetime
import gzip
import re
import subprocess
import time
import sys

class Downloader:

    def __init__(self):
        '''
        A collection of functions for analysis 30 second RINEX files.

        '''


    def remove_unneeded_file(self, local, station, local_filename, gz=False, 
                             crx=False, rnx=False, raport_gfz=False, 
                             raport_bkg=False):
        '''
        :param: local: path to file where is this script
        :param: station: name of station which is analysis
        :param: local_filename: local filename path to downloaded file
        :param: gz, rnx, raport_gfz, raport_bkg: in default False, 
                if True that this file will be removed
        '''
        if gz == True:
            os.remove(local_filename)
        if crx == True:
            os.remove(local_filename[:-3])
        if rnx == True:
            os.remove(f"{local}/{station[:-6]}rnx")
        if raport_gfz == True:
            os.remove(f"{local_filename[:-3]}txt")
        if raport_bkg == True:
            os.remove(f"{local_filename[:-3]}rnx_stk")



    def empty_mgex_df_to_collect_data(self, local):
        '''
        function creates empty dataframe to collecting avability 
        of each satellites for each station by day
        :param local: from this path comes file with MGEX stations 

        :return: empty dataframe with index like satellites PRN and columns 
                like MGEX stations
        '''
        file_sat = f"{local}\data_to_script\sat.txt"
        sat = pd.read_csv(file_sat, header=None)
        arrays = [np.array(sat.iloc[:,0]),
                np.array(sat.iloc[:,1])]

        index = pd.MultiIndex.from_arrays(arrays, names=('sys_name', 'PRN'))
        IGSNetwork = pd.DataFrame(index=index)

        return IGSNetwork


    def empty_quality_df_to_collect_data(self):
        '''
        function creates empty dataframe to collecting quality of each station by day
        :param local: from this path comes file with MGEX stations
        :return: empty dataframe with multiple index and columns
        '''
        stat2 = pd.DataFrame(index=[[""],[""]],
                            columns=[[""],[""]])
        stat2.dropna(how='all', inplace=True, axis=0)
        stat2.dropna(how='all', inplace=True, axis=1)

        return stat2


    def finder(self, directory, phrase):
        '''
        function split text file and find declared phrase
        :param directory: path to file which into will be looking for phrase 
        
        :return: lines of text only where the phrase was detected
        '''
        with open(directory) as logfile:
            splitted_lines = []
            for line in logfile:
                if phrase in line:
                    splitted_lines.append(line.split())
        return splitted_lines

    def looking_for_signal_parameters(self, stat2, local, station):
        '''
        function take from lines data related with signal quality
        :local: path to analysing file
        :station: station which is current analysis
        :param stat2: empty file to this data
        
        :return: dataframe with all statistics relating to signal quality
        '''
        parameters = ["Mean SNR          :", "Observations      :", 
                    "Gaps              :", "Mean Multipath    :"]
        
        names = ['snr', 'obs', 'gaps', 'multipath']

        for par1, name in zip(parameters, names):
            splitted_lines = self.finder(f"{local}\{station[:-6]}txt", par1)
            for line in splitted_lines:
                stat2.loc[(line[0][0], line[1][:2]), (station[:9],name)] = float(line[-1])

        return stat2


    def looking_for_satelite_av(self, df, local, station):
        '''
        function take from declared file data about qavability
        :local: path to analysing file
        :station: station which is current analysis
        :param df: empty file to this data
        
        :return: dataframe with all statistics relating to available
        '''
        with open(f"{local}\{station[:-6]}rnx_stk") as f:
            sats = [re.findall(r"\s[GRECJI][0-9][0-9]\s", line)[0] 
                    for line in f if re.findall(r"\s[GRECJI][0-9][0-9]\s", line)]
            
        for x in sats:
            for y in df.index:
                if (x[1:2],x[1:4]) == y:
                    df.loc[(x[1:2],x[1:4]), station[:9]] = 1 

        df[station[:9]] = df[station[:9]].fillna(value=0)

        return df


    def looking_for_rinex3mo(self, ftps):
        '''
        looking for 30 second rinex file which will be MGEX
        :ftps: connection to the server from which the file is to be downloaded
        
        :return: list with file which will be specify by settings
        '''
        mo_crx_gz = [st for st in ftps.nlst() if st.endswith('01D_30S_MO.crx.gz')]
        
        mgex_file = f"{os.getcwd()}\data_to_script\MGEX_wsp.csv"
        mgex_list = pd.read_csv(mgex_file, index_col=1).index.to_list()
        
        mgex_daily = [_ for _ in mo_crx_gz for i in mgex_list if _[:9] == i]

        return mgex_daily


def main():

    downloader = Downloader()
    print("Processing...")
    local = os.getcwd()
    df = downloader.empty_mgex_df_to_collect_data(local)
    local = os.path.join(local, "data")
    lista = os.listdir(local)
    stat2 = downloader.empty_quality_df_to_collect_data()

    for filename in lista:
        gg = os.path.join(local, filename)
        print(filename)

        try:
            subprocess.check_output(f"cd {local[:-4]} && gfzrnx_1.15.exe /dev/null -finp {gg} -stk_obs -fout {gg[:-3]}rnx_stk /dev/null", shell=True, stderr=subprocess.STDOUT)
        except:
            break
        os.system(f"cd {local[:-4]} && bnc.exe.lnk --nw /dev/null --key reqcAction Analyze --key reqcObsFile {gg[:-3]}rnx --key reqcOutLogFile {gg[:-3]}txt --key reqcLogSummaryOnly 2 --nw")
        stat2 = downloader.looking_for_signal_parameters(stat2, local, filename+"---")
        df = downloader.looking_for_satelite_av(df, local, filename+"---")
        downloader.remove_unneeded_file(local, gg[:-3], gg, gz=False, crx=False, rnx=False, raport_gfz=True, raport_bkg=True)
        
    df.to_csv(f"{local[:-4]}/{gg[-26:-22]}_{gg[-22:-19]}.csv", index = True) 
    stat2.to_csv(f"{local[:-4]}/{gg[-26:-22]}_{gg[-22:-19]}_q.csv", index = True) 
        
if __name__ == "__main__":
    main()
