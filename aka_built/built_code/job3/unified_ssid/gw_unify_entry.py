from unified_ssid.unified_ssid import unify
from datetime import  timedelta
import _datetime


def main(run_date):

        job_3 = unify()
        status = job_3.unified_ssid(run_date)

        if status is True:
            print("Job 3 Done")
        else:
            print("Job 3 crashed")


def run(run_date):

    main(run_date)


