from additional.additional import addition_telemarkers
from datetime import timedelta
import _datetime

def main(run_date):

        job_10 = addition_telemarkers()
        status = job_10.additions(run_date)

        if status is True:
            print("Job 6 Done")
        else:
            print("Job 6 crashed")


def run(run_date):

    main(run_date)