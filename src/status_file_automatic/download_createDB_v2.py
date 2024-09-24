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
import sqlite3
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.set_option('future.no_silent_downcasting', True)
src_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
modules_path = os.path.join(src_path, 'modules')
sys.path.append(modules_path)
from database_manager_SQLite import DatabaseManager


class Downloader:

    def __init__(self, dayS, monthS, yearS, dayE, monthE, yearE):
        '''
        A collection of functions for downloading and analysis 30 second RINEX 
        files for the IGS MGEX network from ftp://gdc.cddis.eosdis.nasa.gov.

        :param dayS: The day from which the program should start downloading data.
        :param monthS: The month from which the program should start downloading data.
        :param yearS: The year from which the program should start downloading data.
        :param dayS: Day in which the program should stop downloading data.
        :param monthS: Month in which the program should stop downloading data.
        :param yearS: Year in which the program should stop downloading data.
        '''
        self.dayS = dayS
        self.monthS = monthS
        self.yearS = yearS
        self.dayE = dayE
        self.monthE = monthE
        self.yearE = yearE


    def start_date(self):
        '''
        :return: Start date to download in date format
        '''
        date  = datetime.datetime(self.yearS, self.monthS, self.dayS)
        return date


    def end_date(self):
        '''
        :return: Start date to download in date format
        '''
        date  = datetime.datetime(self.yearE, self.monthE, self.dayE)
        return date
    

    def login_to_cddis(self, host, user, passwd):
        '''
        :param host: gdc.cddis.eosdis.nasa.gov
        :param user: anonymous
        :param passwd: email
        cddis declares these settings
        :return: An object that is connected to the server.
        '''
        ftps = FTP_TLS(host=host)
        ftps.login(user=user, passwd=passwd)
        return ftps


    def to_directory(self, ftps, year, doy, endpoint):
        '''
        :param ftps: gdc.cddis.eosdis.nasa.gov
        :param year: anonymous
        :param doy: email

        :return: An object that is connected to the server.
        '''
        ftps.cwd(f"/gnss/data/daily/{year}/{doy}/{endpoint}d/")
        ftps.prot_p()
        return ftps


    def progress_bar(self, i, iter_list, station, year, doy, delta):
        '''
        :param i: to control the progress bar
        :param iter_list: list to iterate and make fill bar
        :param station: only for printing which station is analysis
        :param year, doy: only for printing which day is analysis
        :param delta: how many time data from this station downloading

        :return: progress bar
        '''
        percent = ("{0:." + str(1) + "f}").format(100 * (i / float(len(iter_list))))
        filledLength = int((len(iter_list)//4) * (i//4) // (float(len(iter_list))//4))
        bar = '█' * filledLength + '-' * (len(iter_list)//4 - filledLength)
        hours = delta // 3600
        minutes = (delta % 3600) // 60
        seconds = delta % 60
        return print(f"\rStation name: {station[:9]}, YEAR:{year} | DOY:{doy}, \
                     Duration: {str(hours).zfill(2)}:{str(minutes).zfill(2)}:{str(seconds).zfill(2)}   |{bar}| {percent}%", end="\r")


    def copy_rinex_file(self, ftps, full_file_name):
        '''
        :param ftps: connection to the server from which the file is to be downloaded
        :param full_file_name: the name of the file to be downloaded

        :return: local filename path to downloaded file
        '''

        local_filename = os.path.join(os.getcwd(), full_file_name)
        lf = open(local_filename, "wb")  
        ftps.retrbinary('RETR ' + full_file_name, lf.write)
        lf.close()
            
            
        return local_filename


    def ungzip(self, path_in):
        '''
        :param: path_in: path to file which will be unpacked
        '''
        with gzip.open(path_in, 'rb') as f_in:
            with open(path_in[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


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
            os.remove(f"{local}\\{station[:-6]}rnx")
        if raport_gfz == True:
            os.remove(f"{local}\\{station[:-6]}txt")
        if raport_bkg == True:
            os.remove(f"{local}\\{station[:-6]}rnx_stk")



    def empty_mgex_df_to_collect_data(self, database_func):
        '''
        Function creates empty dataframe to collecting avability 
        of each satellites for each station by day.
        :param local: from this path comes file with MGEX stations 

        :return: empty dataframe with index like satellites PRN and columns 
                like MGEX stations
        '''
        sat = database_func.fetch_all('satellites', **{'1': 1})
        sat = pd.DataFrame(sat)
        arrays = [np.array(sat.iloc[:,0]),
                  np.array(sat.iloc[:,1])]

        index = pd.MultiIndex.from_arrays(arrays, names=('sys_name', 'PRN'))
        IGSNetwork = pd.DataFrame(index=index)
        return IGSNetwork


    def empty_quality_df_to_collect_data(self):
        '''
        Function creates empty dataframe to collecting quality of each station by day.
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
        Function split text file and find declared phrase.
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
        Function take from lines data related with signal quality.
        :param local: path to analysing file
        :param station: station which is current analysis
        :param stat2: empty file to this data
        
        :return: dataframe with all statistics relating to signal quality
        '''
        parameters = ["Mean SNR          :", "Observations      :", 
                    "Gaps              :", "Mean Multipath    :"]
        
        names = ['snr', 'obs', 'gaps', 'multipath']

        for par1, name in zip(parameters, names):
            splitted_lines = self.finder(f"{local}\\{station[:-6]}txt", par1)
            for line in splitted_lines:
                stat2.loc[(line[0][0], line[1][:2]), (station[:9],name)] = float(line[-1])

        return stat2


    def looking_for_satelite_av(self, df, local, station):
        '''
        Function take from declared file data about avability.
        :param local: path to analysing file
        :param station: station which is current analysis
        :param df: empty file to this data
        
        :return: dataframe with all statistics relating to available
        '''
        with open(f"{local}\\{station[:-6]}rnx_stk") as f:
            sats = [re.findall(r"\s[GRECJI][0-9][0-9]\s", line)[0] 
                    for line in f if re.findall(r"\s[GRECJI][0-9][0-9]\s", line)]
            
        for x in sats:
            for y in df.index:
                if (x[1:2],x[1:4]) == y:
                    df.loc[(x[1:2],x[1:4]), station[:9]] = 1 

        df[station[:9]] = df[station[:9]].fillna(value=0)

        return df

    
    def looking_for_rinex3mo(self, ftps, database_func):
        '''
        Looking for 30 second rinex file which will be MGEX.
        :param ftps: connection to the server from which the file is to be downloaded
        
        :return: list with file which will be specify by settings
        '''
        mo_crx_gz = [st for st in ftps.nlst() if st.endswith('01D_30S_MO.crx.gz')]
        
        mgex_file = database_func.fetch_all('stations', **{'1': 1})
        mgex_list = pd.DataFrame(mgex_file)
        mgex_list.set_index(mgex_list.columns[1], inplace = True)
        mgex_list = mgex_list.index.tolist()
        
        mgex_daily = [_ for _ in mo_crx_gz for i in mgex_list if _[:9] == i]
        return mgex_daily
    
    
    def availability_to_database(self, dataframe, db_func, station, doy, year):
        '''
        Transfers RINEX file availability data to the database.
        :param dataframe: the dataframe object from which the availability data is taken
        :param db_func: database management function
        :param station: station for which availability data is processed
        :param doy: the day for which the availability data is processed
        :param year: the year for which the availability data is processed
        '''
        for (sys_name, prn) in dataframe.index:
            try:
                availability = dataframe.at[(sys_name, prn), station[:9]]
                table = 'data_availability'
                values = [year, doy, sys_name, prn, station[:9], availability]
                db_func.insert(table, *values)
            except sqlite3.IntegrityError:
                db_func.connection.rollback()
        
        
    def quality_to_database(self, dataframe, db_func, station, doy, year):
        '''
        Transfers RINEX file quality data to the database.
        :param dataframe: the dataframe object from which the quality data is taken
        :param db_func: database management function
        :param station: station for which quality data is processed
        :param doy: the day for which the quality data is processed
        :param year: the year for which the quality data is processed
        '''
        for (sys_name, frequency) in dataframe.index:
            try:
                snr = dataframe.loc[(sys_name, frequency), (station[:9], 'snr')]
                obs = dataframe.loc[(sys_name, frequency), (station[:9], 'obs')]
                gaps = dataframe.loc[(sys_name, frequency), (station[:9], 'gaps')]
                multipath = dataframe.loc[(sys_name, frequency), (station[:9], 'multipath')]
                table = 'data_quality'
                values = [year, doy, sys_name, frequency, station[:9], float(snr), float(obs), float(gaps), float(multipath)]
                db_func.insert(table, *values)
            except sqlite3.IntegrityError:
                db_func.connection.rollback()
                
                
    def update_stations_and_satellites_data(self, update_IGS_Network_file_path, update_Satellite_list_file_path):
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
        finally:
            print("Processing...")   
        

def main():

    day_s = 1
    month_s = 1
    year_s = 2024
    day_e = 1
    month_e = 1
    year_e = 2024
    save_data_to_csv = True
    save_data_to_database = True
    update_data = True
    downloader = Downloader(day_s, month_s, year_s, day_e, month_e, year_e)
    print("Updating data...")
    db_manager = DatabaseManager(database_name = '../database/rinexav_db.db')    
    local = os.getcwd()
    df = downloader.empty_mgex_df_to_collect_data(db_manager)
    date = downloader.start_date()
    lastDate  = downloader.end_date()
    daySteps  = 1 
    ftps = downloader.login_to_cddis('gdc.cddis.eosdis.nasa.gov', 'anonymous', 'email')
    
    update_IGS_Network = os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules/update_IGS_Network_DATABASE.py'))
    update_Satellite_list = os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules/update_Satellite_list_DATABASE.py'))
    
    if update_data:
        downloader.update_stations_and_satellites_data(update_IGS_Network, update_Satellite_list)  
    else:
        print("Processing...")
    
    while date <= lastDate:

        now = datetime.datetime.now()
        year = date.strftime('%Y')
        doy = date.strftime('%j')
        
        endpoint = date.strftime('%y')
        ftps = downloader.to_directory(ftps, year, doy, endpoint)
        stat2 = downloader.empty_quality_df_to_collect_data()
        mgex_daily = downloader.looking_for_rinex3mo(ftps, db_manager)

        i=1
        for station in mgex_daily[:]:
            while True:

                now2 = datetime.datetime.now()
                delta = int((now2 - now).total_seconds())
                downloader.progress_bar(i, mgex_daily, station, year, doy, delta)
                try:
                    local_filename = downloader.copy_rinex_file(ftps, station)    
                    try:
                        downloader.ungzip(local_filename)
                        
                    except:
                        downloader.remove_unneeded_file(local, station, local_filename, gz=True, crx=True)
                        break
                    
                    os.system(f"cd {local} && CRX2RNX_4.1.0 {station[:-3]}")
                    try:
                        subprocess.check_output(f"cd {local} && gfzrnx_2.1.9_win11_64.exe /dev/null -finp {local}\\{station[:-6]}rnx -stk_obs -fout {local}\\{station[:-6]}rnx_stk /dev/null", shell=True, stderr=subprocess.STDOUT)
                    except:
                        break
                    
                    os.system(f"cd {local} && bnc.exe.lnk --nw /dev/null --key reqcAction Analyze --key reqcObsFile {local_filename[:-6]}rnx --key reqcOutLogFile {local_filename[:-6]}txt --key reqcLogSummaryOnly 2 --nw")

                    stat2 = downloader.looking_for_signal_parameters(stat2, local, station)
                    df = downloader.looking_for_satelite_av(df, local, station)
                    
                    if save_data_to_database:
                        downloader.availability_to_database(df, db_manager, station, doy, year)
                        downloader.quality_to_database(stat2, db_manager, station, doy, year)
                    
                    downloader.remove_unneeded_file(local, station, local_filename, gz=True, crx=True, rnx=True, raport_gfz=True, raport_bkg=True)
                    
                except OSError:
                    try:
                        time.sleep(1)
                        #print("Connection was already closed")
                        ftps = downloader.login_to_cddis('gdc.cddis.eosdis.nasa.gov', 'anonymous', 'email')
                        ftps = downloader.to_directory(ftps, year, doy, endpoint)
                        #print("FTP connection has been reset")
                        continue
                            
                    except:
                        print("DIFFERENT ERROR")
                        break
                                        
                except EOFError:
                    try:
                        time.sleep(1)
                        #print("Connection was already closed EOF")
                        ftps = downloader.login_to_cddis('gdc.cddis.eosdis.nasa.gov', 'anonymous', 'email')
                        ftps = downloader.to_directory(ftps, year, doy, endpoint)
                        #print("FTP connection has been reset")
                        continue
                            
                    except:
                        print("DIFFERENT ERROR")
                        break
                break
                
            i+=1
        date += datetime.timedelta(days=daySteps)   
        if save_data_to_csv:
            df.to_csv(f"{local}\\download_data\\{year}_{doy}.csv", index = True) 
            stat2.to_csv(f"{local}\\download_data\\{year}_{doy}_q.csv", index = True)
    
if __name__ == "__main__":
    main()
