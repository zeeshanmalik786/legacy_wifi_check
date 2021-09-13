from gw_process_historical.gw_process_historical import gw_processing
from datetime import timedelta
import _datetime


def main(run_date):

        print("Current Date of Run", run_date)
        job_0 = gw_processing()
        status = job_0.legacy_wifi_check_gw_aggregations_hist(run_date)

        if status is True:
            print("Job 1 Done")
        else:
            print("Job 1 crashed")


def run(run_date):

    main(run_date)

