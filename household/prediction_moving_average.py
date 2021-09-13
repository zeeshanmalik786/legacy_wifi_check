from preprocessor.preprocess import preprocessor
from config.config import config
from pyspark.sql.functions import rank, col, concat
from pyspark.sql.functions import lit


class prediction_moving_average:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def moving_average(self, run_date):
        self.spark.sql("REFRESH TABLE lwc.tmp_single_modem_training_pivot")
        data = self.obj.get_data("lwc.tmp_single_modem_training_pivot", ["gateway_macaddress",
                                                                                "polling_date",
                                                                                "intermittancy_perc",
                                                                                "scaled_accessibility_perc_full_day",
                                                                                "ticket_count",
                                                                                "percent_outlier_far_between_0_to_20",
                                                                                "percent_outlier_far_between_20_to_40",
                                                                                "percent_outlier_far_between_40_to_60",
                                                                                "percent_outlier_far_between_60_to_80",
                                                                                "percent_outlier_far_between_80_to_100",
                                                                                "percent_outlier_near_between_0_to_20",
                                                                                "percent_outlier_near_between_20_to_40",
                                                                                "percent_outlier_near_between_40_to_60",
                                                                                "percent_outlier_near_between_60_to_80",
                                                                                "percent_outlier_near_between_80_to_100",
                                                                                "percent_80dbm_near_between_0_to_20",
                                                                                "percent_80dbm_near_between_20_to_40",
                                                                                "percent_80dbm_near_between_40_to_60",
                                                                                "percent_80dbm_near_between_60_to_80",
                                                                                "percent_80dbm_near_between_80_to_100",
                                                                                "percent_80dbm_far_between_0_to_20",
                                                                                "percent_80dbm_far_between_20_to_40",
                                                                                "percent_80dbm_far_between_40_to_60",
                                                                                "percent_80dbm_far_between_60_to_80",
                                                                                "percent_80dbm_far_between_80_to_100"])

        moving_average_one = self.obj.aggregate_back_n_days(data, ["intermittancy_perc",
                                                          "scaled_accessibility_perc_full_day",
                                                          "ticket_count",
                                                          "percent_outlier_far_between_0_to_20",
                                                          "percent_outlier_far_between_20_to_40",
                                                          "percent_outlier_far_between_40_to_60",
                                                          "percent_outlier_far_between_60_to_80",
                                                          "percent_outlier_far_between_80_to_100",
                                                          "percent_outlier_near_between_0_to_20",
                                                          "percent_outlier_near_between_20_to_40",
                                                          "percent_outlier_near_between_40_to_60",
                                                          "percent_outlier_near_between_60_to_80",
                                                          "percent_outlier_near_between_80_to_100",
                                                          "percent_80dbm_near_between_0_to_20",
                                                          "percent_80dbm_near_between_20_to_40",
                                                          "percent_80dbm_near_between_40_to_60",
                                                          "percent_80dbm_near_between_60_to_80",
                                                          "percent_80dbm_near_between_80_to_100",
                                                          "percent_80dbm_far_between_0_to_20",
                                                          "percent_80dbm_far_between_20_to_40",
                                                          "percent_80dbm_far_between_40_to_60",
                                                          "percent_80dbm_far_between_60_to_80",
                                                          "percent_80dbm_far_between_80_to_100"],
                                                   "polling_date", "gateway_macaddress",
                                                   ["intermittancy_perc",
                                                    "scaled_accessibility_perc_full_day",
                                                    "ticket_count",
                                                    "percent_outlier_far_between_0_to_20",
                                                    "percent_outlier_far_between_20_to_40",
                                                    "percent_outlier_far_between_40_to_60",
                                                    "percent_outlier_far_between_60_to_80",
                                                    "percent_outlier_far_between_80_to_100",
                                                    "percent_outlier_near_between_0_to_20",
                                                    "percent_outlier_near_between_20_to_40",
                                                    "percent_outlier_near_between_40_to_60",
                                                    "percent_outlier_near_between_60_to_80",
                                                    "percent_outlier_near_between_80_to_100",
                                                    "percent_80dbm_near_between_0_to_20",
                                                    "percent_80dbm_near_between_20_to_40",
                                                    "percent_80dbm_near_between_40_to_60",
                                                    "percent_80dbm_near_between_60_to_80",
                                                    "percent_80dbm_near_between_80_to_100",
                                                    "percent_80dbm_far_between_0_to_20",
                                                    "percent_80dbm_far_between_20_to_40",
                                                    "percent_80dbm_far_between_40_to_60",
                                                    "percent_80dbm_far_between_60_to_80",
                                                    "percent_80dbm_far_between_80_to_100"], 7, 1, "minus")

        ticket_moving_average_between_80_to_100 = self.obj.pivot_by_range(moving_average_one, "ticket_count",
                                                                 ["gateway_macaddress", "polling_date"], 1.0, 0.80). \
            withColumnRenamed("count", "ticket_moving_average_between_80_to_100")

        ticket_moving_average_between_60_to_80 = self.obj.pivot_by_range(moving_average_one, "ticket_count",
                                                                ["gateway_macaddress", "polling_date"], 0.8, 0.60). \
            withColumnRenamed("count", "ticket_moving_average_between_60_to_80")

        ticket_moving_average_between_40_to_60 = self.obj.pivot_by_range(moving_average_one, "ticket_count",
                                                                ["gateway_macaddress", "polling_date"], 0.6, 0.40). \
            withColumnRenamed("count", "ticket_moving_average_between_40_to_60")

        ticket_moving_average_between_20_to_40 = self.obj.pivot_by_range(moving_average_one, "ticket_count",
                                                                ["gateway_macaddress", "polling_date"], 0.4, 0.20). \
            withColumnRenamed("count", "ticket_moving_average_between_20_to_40")

        ticket_moving_average_between_0_to_20 = self.obj.pivot_by_range(moving_average_one, "ticket_count",
                                                               ["gateway_macaddress", "polling_date"], 0.2, 0.0). \
            withColumnRenamed("count", "ticket_moving_average_between_0_to_20")

        pivot_ticket_moving_average = self.obj.join_five_frames(ticket_moving_average_between_80_to_100,
                                                       ticket_moving_average_between_60_to_80,
                                                       ticket_moving_average_between_40_to_60,
                                                       ticket_moving_average_between_20_to_40,
                                                       ticket_moving_average_between_0_to_20, "outer",
                                                       ["gateway_macaddress", "polling_date"])

        pivot_ticket_moving_average = pivot_ticket_moving_average.na.fill(0)

        output_pivot = self.obj.join_two_frames(moving_average_one,
                                       pivot_ticket_moving_average,
                                       "outer", ["gateway_macaddress", "polling_date"])

        output_pivot = output_pivot.filter(col("polling_date") == lit(run_date))

        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_prediction_moving_average")

        output_pivot.write.saveAsTable("lwc.tmp_single_modem_prediction_moving_average")

        return True




