from gw_processing.gw_process import gw_process
from device_processing.device_process import device_process
from gw_ethernet.gw_ethernet import gw_ethernet
from algorithm import mean, absolute_deviation, mean_absolute_deviation, bounds, hybrid
from recommendation.recommend import recommendation
from unified_ssid.unify import unify
from additional.additional import addition_telemarkers
import time
from household.intermittency_percentage import intermittency_percentage
from household.pivots_features import pivots_features
from household.moving_average import moving_average
from household.training_set import training_set
from household.training import training
from household.prediction_pivots import prediction_pivots
from household.prediction_moving_average import prediction_moving_average
from household.prediction_classifier import train_test
from datetime import datetime, timedelta
import datetime
from pytz import timezone


def main(run_date):

        print("Current Date of Run", run_date)
        job_1 = gw_process()
        status = job_1.legacy_wifi_check_gw_aggregations(run_date)

        if status is True:
            print("Job 1 Done")
        else:
            print("Job 1 crashed")
        print(time.perf_counter())

        job_2 = gw_ethernet()
        status = job_2.ethernet(run_date)

        if status is True:
            print("Job 2 Done")
        else:
            print("Job 2 crashed")
        print(time.perf_counter())

        job_3 = unify()
        status = job_3.unified_ssid(run_date)

        if status is True:
            print("Job 3 Done")
        else:
            print("Job 3 crashed")
        print(time.perf_counter())

        job_4 = device_process()
        status = job_4.legacy_wifi_check_device_aggregations(run_date)

        if status is True:
            print("Job 4 Done")
        else:
            print("Job 4 crashed")
        print(time.perf_counter())

        job_5 = mean.mean()
        status = job_5.mean()

        if status is True:
            print("Job 5 Done")
        else:
            print("Job 5 crashed")
        print(time.perf_counter())

        job_6 = absolute_deviation.absolute_deviation()
        status = job_6.absolute_deviation()

        if status is True:
            print("Job 6 Done")
        else:
            print("Job 6 crashed")
        print(time.perf_counter())

        job_7 = mean_absolute_deviation.mean_absolute_deviation()
        status = job_7.mean_absolute_deviation()

        if status is True:
            print("Job 7 Done")
        else:
            print("Job 7 crashed")
        print(time.perf_counter())

        job_8 = bounds.bounds()
        status = job_8.bounds()

        if status is True:
            print("Job 8 Done")
        else:
            print("Job 8 crashed")
        print(time.perf_counter())

        job_9 = hybrid.hybrid()
        status = job_9.hybrid()

        if status is True:
            print("Job 9 Done")
        else:
            print("Job 9 crashed")
        print(time.perf_counter())

        job_10 = addition_telemarkers()
        status = job_10.additions()

        if status is True:
            print("Job 10 Done")
        else:
            print("Job 10 crashed")
        print(time.perf_counter())

        job_11 = recommendation()
        status = job_11.recommendation(run_date)

        if status is True:
            print("Job 11 Done")
        else:
            print("Job 11 crashed")
        print(time.perf_counter())

        job_12 = intermittency_percentage()
        status = job_12.households(run_date)

        if status is True:
            print("Job 12 Done")
        else:
            print("Job 12 crashed")
        print(time.perf_counter())

        job_13 = pivots_features()
        status = job_13.pivot_features()

        if status is True:
            print("Job 13 Done")
        else:
            print("Job 13 crashed")
        print(time.perf_counter())
        #
        job_14 = moving_average()
        status = job_14.average()

        if status is True:
            print("Job 14 Done")
        else:
            print("Job 14 crashed")
        print(time.perf_counter())

        job_15 = training_set()
        status = job_15.training()

        if status is True:
            print("Job 15 Done")
        else:
            print("Job 15 Done")
        print(time.perf_counter())

        # job_16 = dimensionality()
        # status = job_16.visualization()
        #
        # if status is True:
        #     print("Job 16 Done")
        # else:
        #     print("Job 16 Done")
        # print(time.perf_counter())

        job_17 = training()
        status = job_17.training()

        if status is True:
            print("Job 17 Done")
        else:
            print("Job 17 Crashed")
        print(time.perf_counter())

        job_18 = prediction_pivots()
        status = job_18.prediction_pivot()

        if status is True:
            print("Job 18 Done")
        else:
            print("Job 18 Crashed")
        print(time.perf_counter())

        job_19 = prediction_moving_average()
        status = job_19.moving_average(run_date)

        if status is True:
            print("Job 18 Done")
        else:
            print("Job 18 Crashed")
        print(time.perf_counter())

        job_20 = train_test()
        status = job_20.train_test()

        if status is True:
            print("Job 20 Done")
        else:
            print("Job 20 Crashed")
        print(time.perf_counter())


if __name__ == '__main__':

    fmt = "%Y-%m-%d"
    tz = timezone('EST')
    current_date = datetime.now(tz).strftime(fmt)
    run_date = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=-1)
    main(run_date)

