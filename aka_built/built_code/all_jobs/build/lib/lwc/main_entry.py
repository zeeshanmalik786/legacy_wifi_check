from lwc.gw_process import gw_process
from lwc.gw_ethernet import gw_ethernet
from lwc.unified_ssid import unify
from lwc.device_process import device_process
from lwc.mean import means
from lwc.mean_absolute_deviation import mean_absolute_deviation
from lwc.bounds import bounds
from lwc.hybrid import hybrid
from lwc.absolute_deviation import absolute_deviation
from lwc.additional import addition_telemarkers
from lwc.recommend import recommendation
from lwc.intermittency_percentage import intermittency_percentage
from lwc.pivots_features import pivots_features
from lwc.moving_average import moving_average
from lwc.training_set import training_sets
from lwc.training import train
from lwc.prediction_pivots import prediction_pivots
from lwc.prediction_moving_average import prediction_moving_average
from lwc.prediction import train_test
import time
from datetime import datetime, timedelta
import datetime
from pytz import timezone


def main(run_date):

        print("Current Date of Run", run_date)

        # Job 1
        job_1 = gw_process()
        status = job_1.legacy_wifi_check_gw_aggregations(run_date)

        if status is True:
            print("Job 1 Done")
        else:
            print("Job 1 crashed")

        # Job 2
        job_2 = gw_ethernet()
        status = job_2.ethernet(run_date)

        if status is True:
            print("Job 2 Done")
        else:
            print("Job 2 crashed")

        # Job 3

        job_3 = unify()
        status = job_3.unified_ssid(run_date)

        if status is True:
            print("Job 3 Done")
        else:
            print("Job 3 crashed")


        # Job 4

        job_4 = device_process()
        status = job_4.legacy_wifi_check_device_aggregations(run_date)

        if status is True:
            print("Job 4 Done")
        else:
            print("Job 4 crashed")


        # Job 5

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


        # Job 6

        job_10 = addition_telemarkers()
        status = job_10.additions(run_date)

        if status is True:
            print("Job 6 Done")
        else:
            print("Job 6 crashed")

        # Job 7

        job_11 = recommendation()
        status = job_11.recommendation(run_date)

        if status is True:
            print("Job 7 Done")
        else:
            print("Job 7 crashed")

        # Job 8

        job_12 = intermittency_percentage()
        status = job_12.households(run_date)

        if status is True:
            print("Job 8 Done")
        else:
            print("Job 8 crashed")
        print(time.perf_counter())

        job_13 = pivots_features()
        status = job_13.pivot_features()

        if status is True:
            print("Job 8_1 Done")
        else:
            print("Job 8_1 crashed")
        print(time.perf_counter())
        #
        job_14 = moving_average()
        status = job_14.average()

        if status is True:
            print("Job 8_2 Done")
        else:
            print("Job 8_2 crashed")
        print(time.perf_counter())

        job_15 = training_sets()
        status = job_15.training_sets()

        if status is True:
            print("Job 8_3 Done")
        else:
            print("Job 8_3 Done")
        print(time.perf_counter())

        job_18 = prediction_pivots()
        status = job_18.prediction_pivot()

        if status is True:
            print("Job 8_5 Done")
        else:
            print("Job 8_5 Crashed")
        print(time.perf_counter())

        job_19 = prediction_moving_average()
        status = job_19.moving_average(run_date)

        if status is True:
            print("Job 8_6 Done")
        else:
            print("Job 8_6 Crashed")
        print(time.perf_counter())

        job_20 = train_test()
        status = job_20.train_test()

        if status is True:
            print("Job 8_7 Done")
        else:
            print("Job 8_7 Crashed")
        print(time.perf_counter())


def run(run_date):

    main(run_date)
