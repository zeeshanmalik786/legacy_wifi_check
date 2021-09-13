from device_processing.device_process import device_process
from datetime import  timedelta
import _datetime


def main(run_date):

        job_4 = device_process()
        status = job_4.legacy_wifi_check_device_aggregations(run_date)

        if status is True:
            print("Job 4 Done")
        else:
            print("Job 4 crashed")


def run(run_date):
    main(run_date)


