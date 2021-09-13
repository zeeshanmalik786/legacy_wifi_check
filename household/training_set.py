from preprocessor.preprocess import preprocessor
from config.config import config
from pyspark.sql.functions import rank, col
from pyspark.sql.functions import lit


class training_set:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.positive_examples = 50000
        self.negative_examples = 50000

    def training(self):
        self.spark.sql("REFRESH TABLE lwc.tmp_single_modem_moving_averages")
        data = self.obj.get_data("lwc.tmp_single_modem_moving_averages", ["gateway_macaddress",
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
                                                                                 "percent_80dbm_far_between_80_to_100",
                                                                                 "ticket_moving_average_between_80_to_100",
                                                                                 "ticket_moving_average_between_60_to_80",
                                                                                 "ticket_moving_average_between_40_to_60",
                                                                                 "ticket_moving_average_between_20_to_40",
                                                                                 "ticket_moving_average_between_0_to_20"])
        # define negative examples having atleast ticket > 0 and intermittancy_perc > 0.80
        # zero means customer needs a pod
        negative_examples = data.filter((col("ticket_count") > 0) & (
                    (col("intermittancy_perc") >= 0.60) | (col("scaled_accessibility_perc_full_day") < 0.95))). \
            sort(col("percent_outlier_near_between_0_to_20").desc()).limit(self.negative_examples). \
            withColumn("label", lit(0))

        # define positive examples having atleast ticket <= 0 and intermittancy_perc < 0.20
        # one means customer do not need a pod
        positive_examples = data.filter((col("ticket_count") <= 0) & (
                    (col("intermittancy_perc") <= 0.20) | (col("scaled_accessibility_perc_full_day") >= 1))). \
            sort(col("percent_outlier_near_between_80_to_100").desc()).limit(self.positive_examples). \
            withColumn("label", lit(1))

        # define negative examples having atleast  intermittancy_perc > 0.80
        # negative_examples = data.filter((col("ticket_count") > 0)).\
        #             limit(negative_examples).\
        #             withColumn("label", lit(0))

        # define positive examples having atleast ticket <= 0 and intermittancy_perc < 0.20
        # positive_examples = data.filter((col("ticket_count") <= 0)).\
        #             limit(positive_examples).\
        #             withColumn("label", lit(1))

        training_data = positive_examples.union(negative_examples)
        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_training_set")
        training_data.write.saveAsTable("lwc.tmp_single_modem_training_set")
