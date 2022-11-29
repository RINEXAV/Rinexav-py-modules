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

def dividing_stations(IGSNetwork, alg, num_points):
    if alg == "KMeans":
        algorytm = KMeans(n_clusters=num_points, random_state=0)
        
    elif alg == "AgglomerativeClustering":
        algorytm = AgglomerativeClustering(n_clusters=num_points)
    
    labels = algorytm.fit(np.array(IGSNetwork.iloc[:,:3])).labels_
    labels = pd.DataFrame(labels, index=IGSNetwork.index,columns=['segment'])
    IGSNetwork = pd.concat([IGSNetwork, labels], axis=1)
    
    return IGSNetwork

#Multiple-criteria decision analysis
def MDCA(IGSNetwork, alg, weights, num_points, num_hz):
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

        if alg == "TOPSIS":
            rank = top.topsis(evaluation_matrix, weights, criterias)
            df = pd.DataFrame(rank, index=seg.index, columns=["TOPSIS"])
            
        elif alg == "COPRAS":
            rank = cop.copras_method(evaluation_matrix, weights, criterias2)
            df = pd.DataFrame(rank, index=seg.index, columns=["COPRAS"])
        
        mat = pd.concat([mat, df])
    IGSNetwork = pd.concat([IGSNetwork, mat], axis=1)

    return IGSNetwork

def only_ones(IGSNetwork, method):
    IGSNetwork = IGSNetwork[IGSNetwork.loc[:,method]==1]
    return IGSNetwork

def mean_all(out, freq_done1):
    full_mean = pd.DataFrame()
    par = ["snr", "obs", "gaps", "multipath"]
    for hh in freq_done1:
        for x in par:
            d = pd.DataFrame(out.loc[:, (hh[0], hh[1], x)].T.mean(), columns=[(hh[0],hh[1],x)])
            full_mean = pd.concat([full_mean, d], axis=1)
    return full_mean

def process_file(IGSNetwork, folder_name, date, lastDate):
    print("Przetwarzam pliki...")
    snr_mean2 = pd.DataFrame()
    gaps_mean2 = pd.DataFrame()
    obs_mean2 = pd.DataFrame()
    mp_mean2 = pd.DataFrame()

    sys = []
    i=0
    while date <= lastDate:
        dirname = os.path.dirname(os.path.abspath("__file__"))
        directory = os.path.join(dirname,folder_name)
        
        file_to_av = dir_to_pick_file(directory, date, ".csv")
        file_to_q = dir_to_pick_file(directory, date, "_q.csv")
        
        plik = pd.read_csv(file_to_av, index_col=0)
        plik2 = pd.read_csv(file_to_q, index_col=[0,1], header=[0,1])
        plik4 = pd.read_csv(file_to_q).iloc[:,0].to_list()

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
