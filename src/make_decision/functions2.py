import os
import pandas as pd
import numpy as np 
from matplotlib import pyplot as plt
import datetime
from sklearn.cluster import KMeans, AgglomerativeClustering
import topsis_FG as top
import copras as cop


def how_empty(file, IGSNetwork):
    for z in file.columns:
        if file[z].sum() == 0:
            try:
                IGSNetwork.loc[z,'stats'] = IGSNetwork.loc[z,'stats'] + 1
            except(KeyError):
                continue
    return IGSNetwork


def dir_to_pick_file(directory, date, endpoint):
    
    year = date.strftime('%Y')
    doy = date.strftime('%j')
    list_of_file = os.listdir(directory)
    s = []
    for k in list_of_file:
        if k.endswith(f"{year}_{doy}{endpoint}"):
            s.append(os.path.join(directory, k))
    return s[0]


def dividing_stations(IGSNetwork, num_points):

    algorytm = KMeans(n_clusters=num_points, random_state=0)
    
    labels = algorytm.fit(np.array(IGSNetwork.iloc[:,:3])).labels_
    labels = pd.DataFrame(labels, index=IGSNetwork.index,columns=['segment'])
    IGSNetwork = pd.concat([IGSNetwork, labels], axis=1)
    
    return IGSNetwork


#Multiple-criteria decision analysis
def MDCA(IGSNetwork, weights, num_points, num_hz):
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


def only_ones(IGSNetwork):
    IGSNetwork = IGSNetwork[IGSNetwork.loc[:,"TOPSIS"]==1]
    return IGSNetwork


def mean_all(out, freq_done1):
    full_mean = pd.DataFrame()
    par = ["snr", "obs", "gaps", "multipath"]
    for hh in freq_done1:
        for x in par:
            d = pd.DataFrame(out.loc[:, (hh[0], hh[1], x)].T.mean(), columns=[(hh[0],hh[1],x)])
            full_mean = pd.concat([full_mean, d], axis=1)
    return full_mean


def process_file(IGSNetwork, db_func, date, lastDate):
    print("Przetwarzam pliki...")
    snr_mean2 = pd.DataFrame()
    gaps_mean2 = pd.DataFrame()
    obs_mean2 = pd.DataFrame()
    mp_mean2 = pd.DataFrame()

    sys = []
    i=0
    while date <= lastDate:

        conditions = {'year': date.year, 'day_of_year': date.timetuple().tm_yday}
        data_availability = db_func.fetch_all('data_availability', **conditions)
        data_quality = db_func.fetch_all('data_quality', **conditions)
                
        plik = pd.DataFrame(data_availability)
        plik = plik.iloc[:, [2,3,4,5]]
        plik.columns = ['sys_name', 'PRN', 'station_name', 'availability']
        unique_pairs = plik[['sys_name', 'PRN']].drop_duplicates()
        plik = plik.pivot_table(index = ['sys_name', 'PRN'], columns = 'station_name', values = 'availability', dropna = False)
        plik = plik.loc[unique_pairs.set_index(['sys_name', 'PRN']).index]
        
        plik2 = pd.DataFrame(data_quality)
        plik2 = plik2.iloc[:, [2,3,4,5,6,7,8]]
        plik2.columns = ['sys_name', 'frequency', 'station_name', 'snr', 'obs', 'gaps', 'multipath']
        plik2.set_index(['sys_name', 'frequency', 'station_name'], inplace=True)
        plik2 = plik2.unstack(level='station_name')
        plik2.columns = pd.MultiIndex.from_tuples(plik2.columns, names=['station_name', 'metric'])
        plik2.columns = plik2.columns.swaplevel('metric', 'station_name')
        plik2 = plik2.sort_index(axis=1)
        
        plik4 = plik2.reset_index().iloc[:, 0].to_list()
        
        for y in plik4:
            if type(y) is str:
                sys.append(y)
        IGSNetwork = how_empty(plik, IGSNetwork)
        
        snr2 = plik2.stack().T.loc[:, (slice(None), slice(None), "snr")]
        snr_mean2 = pd.concat([snr_mean2, snr2], axis=1)

        gaps2 = plik2.stack().T.loc[:, (slice(None), slice(None), "gaps")]
        gaps_mean2 = pd.concat([gaps_mean2, gaps2], axis=1)
        
        obs2 = plik2.stack().T.loc[:, (slice(None), slice(None), "obs")]
        obs_mean2 = pd.concat([obs_mean2, obs2], axis=1)
        
        multipath2 = plik2.stack().T.loc[:, (slice(None), slice(None), "multipath")]
        mp_mean2 = pd.concat([mp_mean2, multipath2], axis=1)
        
        i+=1
        date += datetime.timedelta(days=1)

    df = pd.concat([snr_mean2, obs_mean2, gaps_mean2, mp_mean2], axis=1)
    sys = sorted(list(set(sys)))

    return df, i, sys