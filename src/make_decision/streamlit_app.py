import streamlit as st
from streamlit_folium import folium_static
import folium
import pandas as pd
import base64
import datetime
import os
from functions2 import *
import branca.colormap as cm
import copy
import subprocess
import time
import sys

src_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
modules_path = os.path.join(src_path, 'modules')
sys.path.append(modules_path)
from database_manager_SQLite import DatabaseManager


def filedownload(df, name):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # strings <-> bytes conversions
    href = f'<a href="data:file/csv;base64,{b64}" download="{name}.csv">Download CSV File</a>'
    return href


st.set_page_config(
    page_title="RINEX-AV",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded")

t1, t2, t3= st.columns((1,0.05, 0.15)) 
t3.image('upwr.png', width=300)
t1.image('igig.png', width=100)
st.markdown("<h2 style='text-align: center; color: black;'>GNSS permanent MGEX station selection system based on qualitative analysis of RINEX files</h2>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: black;'>The application selects the stations with the best quality data in the RINEX file.</h3>", unsafe_allow_html=True)

col1 = st.sidebar
col2, col3 = st.columns((2,1))

db_manager = DatabaseManager(database_name = '../database/rinexav_db.db')
file = db_manager.fetch_all('stations', **{'1': 1})
IGSNetwork = pd.DataFrame(file)
IGSNetwork = IGSNetwork.iloc[:, [1, 2, 3, 4, 5, 6, 7, 9]]
IGSNetwork.columns = ['#StationName', 'X', 'Y', 'Z', 'Latitude', 'Longitude', 'Height', 'pr_level']
IGSNetwork = IGSNetwork.set_index('#StationName')
IGSNetwork['stats'] = 0

with st.expander("HOW TO USE"):
        st.write("""
                 -\t1. Specify the time period for the analysis (Start date, End date).\n
                 -\t2. Specify the availability of RINEX files during this time period (Avarage Percent of RINEX availability).\n
                 -\t3. The clustering method - KMeans - is selected automatically by the software.\n
                 -\t4. The decision-making method - TOPSIS - is selected automatically by the software.\n
                 -\t5. Enter the number of stations evenly distributed around the world (Number of Stations).\n
                 -\t6. To proceed check the box and wait for a while.\n
                 -\t7. Select system frequencies and transmit channels (Systems, Frequencies). Wait for a while.\n
                 -\t8. Specify the weight of each parameter (WEIGHT).\n
                 -\t9. Please wait for a while.\n
                 Priority level is determined using the LIST OF STATION PROPOSED TO IGS REPRO3, which can be found [here](http://acc.igs.org/repro3/repro3_station_priority_list_060819.pdf).\n""")

st.sidebar.info('Click to update the list of IGS MGEX stations, their coordinates and satellite PRN numbers', icon = '⬇️')
update_data = st.sidebar.button('UPDATE DATA', use_container_width = True)

update_IGS_Network = os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules/update_IGS_Network_DATABASE.py'))
update_Satellite_list = os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules/update_Satellite_list_DATABASE.py'))

if update_data:
    try:        
        igs_message = st.sidebar.info('Updating IGS Network data...')
        IGS_Network_list_update = subprocess.run(['python', update_IGS_Network], check = True, capture_output = True, text = True)
        igs_message.empty()
        sat_message = st.sidebar.info('Updating satellite PRNs data...')
        Satellite_list_update = subprocess.run(['python', update_Satellite_list], check = True, capture_output = True, text = True)
        sat_message.empty()
        results_message = st.sidebar.success('Data has been updated successfully!')
    except subprocess.CalledProcessError:
        results_message = st.sidebar.error('An error occurred while updating the data!')
    finally:
        time.sleep(3)
        st.rerun()

date = st.sidebar.date_input('Start date', datetime.date(2022,1,1))

if date:
    lastDate = st.sidebar.date_input('End date', datetime.date(2022,1,15))
    if date < lastDate:
        ileprocent = st.sidebar.slider('Avarage Percent of RINEX availability', 30, 100, value=90, step=1)
        num_points = st.sidebar.slider('Number of Stations', 5, IGSNetwork.shape[0], value=100, step=1)
        
    else:      
        st.error("ENTER THE RELEVANT DATE RANGE")
        st.stop()

if st.sidebar.checkbox('<-- Load further parameters'):
    try:
        df, i, sys = process_file(IGSNetwork, db_manager, date, lastDate)
    except IndexError:
        st.warning("NO DATA AVAILABLE FOR SELECTED DATE RANGE")
        st.stop()

    with col1:
        SORT_ORDER_GNSS = ["G", "R", "E", "C", "J", "I", "S"]
        sys.sort(key = lambda x: SORT_ORDER_GNSS.index(x))
        sys_bar1 = st.sidebar.multiselect('Systems', sys, default=sys[0])
        
        freq1 = df.loc[:, (sys_bar1, slice(None), slice(None))].columns.to_list()
        
    hz = []
    for par in freq1:
        hz.append(tuple(list(par)[:2]))


    freq1 = sorted(list(set(hz)))
    freq1.sort(key = lambda x: SORT_ORDER_GNSS.index(x[0]))
    if freq1:
        freq_done1 =  st.sidebar.multiselect('Frequencies', freq1, default=[freq1[0]])
        par = ['SNR', 'Number of obsevations', 'GAPS', 'MULTIPATH']
        st.sidebar.write('WEIGHT:')
        k = int(st.sidebar.number_input('Priority Level', 1, 100))
        ich_all=[k]
        
        for h in par:
            ich = int(st.sidebar.number_input(f'{h}', 1, 100))
            ich_all.append(ich)
                
    if st.sidebar.button('Submit'):
        outer2=pd.DataFrame()
        
        for x in freq_done1:
            out = df.loc[:, (x[0], x[1], slice(None))]
            outer2 = pd.concat([outer2, out], axis=1, join='outer')
        
        outer2 = outer2.dropna(how='all')
        
        full_mean = pd.DataFrame()
        par = ["snr", "obs", "gaps", "multipath"]
        for hh in freq_done1:
            for x in par:
                d = pd.DataFrame(outer2.loc[:, (hh[0], hh[1], x)].T.mean(), columns=[(hh[0],hh[1],x)])
                full_mean = pd.concat([full_mean, d], axis=1, join='outer')
                
        IGSNetwork2 = pd.concat([IGSNetwork, full_mean], axis=1, join='inner')
        
        par = IGSNetwork2.iloc[:,8:]
        got = copy.copy(par)

        n=0
        for pars in range(len(freq_done1)-1):
            rename_dic = dict(zip(par.columns[4+n:8+n], par.columns[:4]))
            out = par.iloc[:, :4].combine_first(par.iloc[:, 4+n:8+n].rename(columns=rename_dic))
            got.update(got.iloc[:, 4+n:8+n].set_axis(got.columns[:4], axis=1), overwrite=False)
            n+=4
        
        n=4
        for pyk in range(len(freq_done1)-1):
            hahs = got.iloc[:, n:n+4].loc[got.loc[:, [(freq_done1[0][0], freq_done1[0][1], 'obs')]].values < got.loc[:, [(freq_done1[1+pyk][0], freq_done1[1+pyk][1], 'obs')]].values]
            got.iloc[:, :4].loc[hahs.index] = hahs
            n+=4

        ready = got.iloc[:,:4]
        ready.columns = ['SNR', 'OBS', 'GAPS', 'MULTIPATH']
        
        IGSNetwork = pd.concat([IGSNetwork2.iloc[:,:8], ready], axis=1)
        IGSNetwork.iloc[:,7:8] = ((i-IGSNetwork.iloc[:,7:8])/i) *100
        IGSNetwork = IGSNetwork[IGSNetwork.loc[:,"stats"]>=ileprocent]
            
        if IGSNetwork.shape[0] >= num_points:
            IGSNetwork = dividing_stations(IGSNetwork, num_points)
            IGSNetwork = MDCA(IGSNetwork, ich_all, num_points, len(freq_done1))
            wybor = only_ones(IGSNetwork)
            wybor = wybor.iloc[:,3:]
            IGSNetwork = IGSNetwork.iloc[:,3:]
            
            col2.markdown(f"<h4 color: black;'>All stations that have RINEX file availability above {ileprocent}% and have observations on selected frequencies.</h4>", unsafe_allow_html=True)
            col2.dataframe(IGSNetwork.style.format("{:.1f}"), height=380)
            col2.write('Data Dimension: ' + str(IGSNetwork.shape[0]) + ' rows and ' + str(IGSNetwork.shape[1]) + ' columns.')
            col2.markdown(filedownload(IGSNetwork.reset_index(), 'all_stations'), unsafe_allow_html=True)
            col2.markdown(f"<h4 color: black;'>Selected {num_points} stations with the best RINEX file quality.</h4>", unsafe_allow_html=True)
            col2.dataframe(wybor.style.format("{:.1f}"), height=380)
            col2.write('Data Dimension: ' + str(wybor.shape[0]) + ' rows and ' + str(wybor.shape[1]) + ' columns.')
            col2.markdown(filedownload(wybor.reset_index(), "selected"), unsafe_allow_html=True)
            
            df = IGSNetwork.loc[:,["Longitude", "Latitude"]]
            df = df.rename(columns={"Longitude": "lon", "Latitude": "lat"})
            
            df2 = wybor.loc[:,["Longitude", "Latitude"]]
            df2 = df2.rename(columns={"Longitude": "lon", "Latitude": "lat"})
    
            m = folium.Map()
            m2 = folium.Map()
            
            df3 =  IGSNetwork.loc[:,["Longitude", "Latitude", "segment"]]
            df3 = df3.rename(columns={"Longitude": "lon", "Latitude": "lat"})
            linear = cm.LinearColormap(["r", "y", "g", "c", "b", "m","white", "black", "red"], vmin=df3.loc[:,'segment'].min(), vmax=df3.loc[:,'segment'].max())

            for _, row in df3.iterrows():
                pos = [row.lat, row.lon]
                folium.CircleMarker(pos,
                            popup=_,
                            radius=3,
                            color = linear(row.segment)
                            ).add_to(m)
                
            for x in df2.index:
                pos = list(np.flip(np.array(df2.loc[x])))
                folium.CircleMarker(pos,
                            popup=x,
                            radius=2,
                            fill_color="red", 
                            color = "red",
                            ).add_to(m2)

            with col3:
                folium_static(m)
                folium_static(m2)
        elif IGSNetwork.shape[0] == 0:
            st.markdown(f"<h3 style='text-align: center; color: red;'>None of the stations meet the conditions entered.</h3>", unsafe_allow_html=True)
            
        else:
            st.dataframe(IGSNetwork)
            st.markdown(f"<h3 style='text-align: center; color: red;'>Only {IGSNetwork.shape[0]} stations received a signal on selected frequencies. To display the result, decrease the number of stations.</h3>", unsafe_allow_html=True)
            
    st.sidebar.title("About")
    st.sidebar.info("""This web [app](##) is maintained by [Filip Gałdyn](##). You can follow me on social media:
                    [GitHub](https://github.com/filipgaldyn),
                    [Twitter](https://twitter.com/FilipGaldyn),
                    [LinkedIn](https://www.linkedin.com/in/filip-ga%C5%82dyn/).
                    This web app URL: <https://rinexav.herokuapp.com/>""")
