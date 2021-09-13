from household.intermittency_percentage import intermittency_percentage
from household.pivots_features import pivots_features
from household.moving_average import moving_average
from household.training_set import training_sets
from household.training import train
from household.prediction_pivots import prediction_pivots
from household.prediction_moving_average import prediction_moving_average
from household.prediction import train_test
import time


def main(run_date):

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

        # job_17 = train()
        # status = job_17.train()
        #
        # if status is True:
        #     print("Job 8_4 Done")
        # else:
        #     print("Job 8_4 Crashed")
        # print(time.perf_counter())

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