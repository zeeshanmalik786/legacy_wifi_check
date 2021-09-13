from recommendation.recommend import recommendation
from datetime import  timedelta
import _datetime


def main(run_date):

        job_11 = recommendation()
        status = job_11.recommendation(run_date)

        if status is True:
            print("Job 7 Done")
        else:
            print("Job 7 crashed")

def run(run_date):

    main(run_date)