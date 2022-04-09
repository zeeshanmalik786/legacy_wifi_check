from gw_process.gw_process import gw_process
from datetime import timedelta
import _datetime


def main(run_date, unite):

        print("Current Date of Run", run_date)
        job_1 = gw_process()

        status = job_1.legacy_wifi_check_gw_aggregations(run_date, unite)

        if status is True:
            print("Job 1 Done")
        else:
            print("Job 1 crashed")


def run(run_date, unite):

    main(run_date,unite)

