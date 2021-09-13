<h1> Steps to Run Historical Run </h1>

1. Install the built file located at legacy_wifi_check_v1/aka_built/job0/dist/gw_process_historical-0.1-py2.7.egg
2. Copy the below code in databricks notebook

````
# Importing Defined Packages
from gw_process_historical import gw_entry; 
from datetime import timedelta
import _datetime

run_date = _datetime.date.today() + timedelta(days=-2)

if __name__=='__main__':
  gw_entry.run(run_date)    
  
````

3. Check the output table in LWC container (The output table name will be lwc.tmp_single_modems_historical_run)



