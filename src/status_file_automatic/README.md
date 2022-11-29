# download_createDB_v2.py
<h2>Module to download files from CDDIS server from IGS MGEX station network and generating two daily status  with: quality and availability</h2><br>
<h3>The running of the script is possible after installing <a href=: "https://igs.bkg.bund.de/ntrip/bnc">BKG NTrip Client</a>.</h3>
<li>main file to run script: download_createDB_v2.py </li>
<li>dataset uses in software: data_to_script (IGS MGEX list, station info, satPRN) </li>
<h3>Processing diagram</h3><br>
<img src="modul1_scheme.png" alt="modul1_scheme" width="400" height="500"><br>
<h4>Example input in status_file_from_folder.py:</h4>
<li>day_s = 1     #day from which the script should start downloading data</li>
<li>month_s = 11  #month from which the script should start downloading data </li>
<li>year_s = 2022 #year from which the script should start downloading data</li>
<li>day_e = 1     #day from which the script should end downloading data</li>
<li>month_e = 11  #month from which the script should end downloading data</li>
<li>year_e = 2022 #year from which the script should end downloading data</li>

<h4>Software export csv file with:</h4>
<li>avability daily status file</li>
<li>quality daily status file</li>