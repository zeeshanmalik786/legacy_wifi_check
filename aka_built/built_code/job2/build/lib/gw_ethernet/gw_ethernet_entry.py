from gw_ethernet.gw_ethernet import gw_ethernet
from datetime import  timedelta
import _datetime
from pytz import timezone

def main(run_date):

        job_2 = gw_ethernet()
        status = job_2.ethernet(run_date)

        if status is True:
            print("Job 2 Done")
        else:
            print("Job 2 crashed")

def run(run_date):

    main(run_date)


