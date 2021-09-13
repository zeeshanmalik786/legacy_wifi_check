from algorithm.mean import means
from algorithm.mean_absolute_deviation import mean_absolute_deviation
from algorithm.bounds import bounds
from algorithm.hybrid import hybrid
from algorithm.absolute_deviation import absolute_deviation
from datetime import  timedelta
import _datetime
import time


def main(run_date):
    job_5 = means()
    status = job_5.mean_one()

    if status is True:
        print("Job 5 Done")
    else:
        print("Job 5 crashed")
    print(time.perf_counter())

    job_6 = absolute_deviation()
    status = job_6.absolute_deviation()

    if status is True:
        print("Job 5_1 Done")
    else:
        print("Job 5_1 crashed")
    print(time.perf_counter())

    job_7 = mean_absolute_deviation()
    status = job_7.mean_absolute_deviation()

    if status is True:
        print("Job 5_2 Done")
    else:
        print("Job 5_2 crashed")
    print(time.perf_counter())

    job_8 = bounds()
    status = job_8.bounds()

    if status is True:
        print("Job 5_3 Done")
    else:
        print("Job 5_3 crashed")
    print(time.perf_counter())

    job_9 = hybrid()
    status = job_9.hybrid(run_date)

    if status is True:
        print("Job 5_4 Done")
    else:
        print("Job 5_4 crashed")
    print(time.perf_counter())


def run(run_date):

    main(run_date)
