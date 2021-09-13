from preprocessor.preprocess import preprocessor
from config.config import config
from pyspark.sql import functions as func


class pivots_features:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def pivot_features(self):

        data = self.obj.get_data("business_users_test.tmp_single_modem_intermittancy_percentage", ["gateway_macaddress",
                                                                                          "associated_device_macaddress",
                                                                                          "polling_date",
                                                                                          "intermittancy_perc",
                                                                                          "percent_outlier_far",
                                                                                          "percent_outlier_near",
                                                                                          "percent_80dbm_near",
                                                                                          "percent_80dbm_far",
                                                                                          "scaled_accessibility_perc_full_day",
                                                                                          "ticket_count"])

        # pivots the features based on ranges

        # percent_outlier_far

        percent_outlier_far_between_80_to_100 = self.obj.pivot_by_range(data, "percent_outlier_far",
                                                               ["gateway_macaddress", "polling_date"], 1.0, 0.80). \
            withColumnRenamed("count", "percent_outlier_far_between_80_to_100")

        percent_outlier_far_between_60_to_80 = self.obj.pivot_by_range(data, "percent_outlier_far",
                                                              ["gateway_macaddress", "polling_date"], 0.8, 0.60). \
            withColumnRenamed("count", "percent_outlier_far_between_60_to_80")

        percent_outlier_far_between_40_to_60 = self.obj.pivot_by_range(data, "percent_outlier_far",
                                                              ["gateway_macaddress", "polling_date"], 0.6, 0.40). \
            withColumnRenamed("count", "percent_outlier_far_between_40_to_60")

        percent_outlier_far_between_20_to_40 = self.obj.pivot_by_range(data, "percent_outlier_far",
                                                              ["gateway_macaddress", "polling_date"], 0.4, 0.20). \
            withColumnRenamed("count", "percent_outlier_far_between_20_to_40")

        percent_outlier_far_between_0_to_20 = self.obj.pivot_by_range(data, "percent_outlier_far",
                                                             ["gateway_macaddress", "polling_date"], 0.2, 0.0). \
            withColumnRenamed("count", "percent_outlier_far_between_0_to_20")

        pivot_percent_outlier_far = self.obj.join_five_frames(percent_outlier_far_between_80_to_100,
                                                     percent_outlier_far_between_60_to_80,
                                                     percent_outlier_far_between_40_to_60,
                                                     percent_outlier_far_between_20_to_40,
                                                     percent_outlier_far_between_0_to_20, "outer",
                                                     ["gateway_macaddress", "polling_date"])

        pivot_percent_outlier_far = pivot_percent_outlier_far.na.fill(0)

        # percent_outlier_near

        percent_outlier_near_between_80_to_100 = self.obj.pivot_by_range(data, "percent_outlier_near",
                                                                ["gateway_macaddress", "polling_date"], 1.0, 0.80). \
            withColumnRenamed("count", "percent_outlier_near_between_80_to_100")

        percent_outlier_near_between_60_to_80 = self.obj.pivot_by_range(data, "percent_outlier_near",
                                                               ["gateway_macaddress", "polling_date"], 0.8, 0.60). \
            withColumnRenamed("count", "percent_outlier_near_between_60_to_80")

        percent_outlier_near_between_40_to_60 = self.obj.pivot_by_range(data, "percent_outlier_near",
                                                               ["gateway_macaddress", "polling_date"], 0.6, 0.40). \
            withColumnRenamed("count", "percent_outlier_near_between_40_to_60")

        percent_outlier_near_between_20_to_40 = self.obj.pivot_by_range(data, "percent_outlier_near",
                                                               ["gateway_macaddress", "polling_date"], 0.4, 0.20). \
            withColumnRenamed("count", "percent_outlier_near_between_20_to_40")

        percent_outlier_near_between_0_to_20 = self.obj.pivot_by_range(data, "percent_outlier_near",
                                                              ["gateway_macaddress", "polling_date"], 0.2, 0.0). \
            withColumnRenamed("count", "percent_outlier_near_between_0_to_20")

        pivot_percent_outlier_near = self.obj.join_five_frames(percent_outlier_near_between_80_to_100,
                                                      percent_outlier_near_between_60_to_80,
                                                      percent_outlier_near_between_40_to_60,
                                                      percent_outlier_near_between_20_to_40,
                                                      percent_outlier_near_between_0_to_20, "outer",
                                                      ["gateway_macaddress", "polling_date"])
        pivot_percent_outlier_near = pivot_percent_outlier_near.na.fill(0)

        # percent_80dbm_near

        percent_80dbm_near_between_80_to_100 = self.obj.pivot_by_range(data, "percent_80dbm_near",
                                                              ["gateway_macaddress", "polling_date"], 1.0,
                                                              0.80). \
            withColumnRenamed("count", "percent_80dbm_near_between_80_to_100")

        percent_80dbm_near_between_60_to_80 = self.obj.pivot_by_range(data, "percent_80dbm_near",
                                                             ["gateway_macaddress", "polling_date"], 0.8,
                                                             0.60). \
            withColumnRenamed("count", "percent_80dbm_near_between_60_to_80")

        percent_80dbm_near_between_40_to_60 = self.obj.pivot_by_range(data, "percent_80dbm_near",
                                                             ["gateway_macaddress", "polling_date"], 0.6,
                                                             0.40). \
            withColumnRenamed("count", "percent_80dbm_near_between_40_to_60")

        percent_80dbm_near_between_20_to_40 = self.obj.pivot_by_range(data, "percent_80dbm_near",
                                                             ["gateway_macaddress", "polling_date"], 0.4,
                                                             0.20). \
            withColumnRenamed("count", "percent_80dbm_near_between_20_to_40")

        percent_80dbm_near_between_0_to_20 = self.obj.pivot_by_range(data, "percent_80dbm_near",
                                                            ["gateway_macaddress", "polling_date"], 0.2,
                                                            0.0). \
            withColumnRenamed("count", "percent_80dbm_near_between_0_to_20")

        pivot_percent_80dbm_near = self.obj.join_five_frames(percent_80dbm_near_between_80_to_100,
                                                    percent_80dbm_near_between_60_to_80,
                                                    percent_80dbm_near_between_40_to_60,
                                                    percent_80dbm_near_between_20_to_40,
                                                    percent_80dbm_near_between_0_to_20, "outer",
                                                    ["gateway_macaddress", "polling_date"])

        pivot_percent_80dbm_near = pivot_percent_80dbm_near.na.fill(0)

        # percent_80dbm_far

        percent_80dbm_far_between_0_to_20 = self.obj.pivot_by_range(data, "percent_80dbm_far",
                                                           ["gateway_macaddress", "polling_date"], 0.2,
                                                           0.0). \
            withColumnRenamed("count", "percent_80dbm_far_between_0_to_20")
        percent_80dbm_far_between_20_to_40 = self.obj.pivot_by_range(data, "percent_80dbm_far",
                                                            ["gateway_macaddress", "polling_date"], 4.0,
                                                            0.20). \
            withColumnRenamed("count", "percent_80dbm_far_between_20_to_40")
        percent_80dbm_far_between_40_to_60 = self.obj.pivot_by_range(data, "percent_80dbm_far",
                                                            ["gateway_macaddress", "polling_date"], 0.6,
                                                            0.40). \
            withColumnRenamed("count", "percent_80dbm_far_between_40_to_60")
        percent_80dbm_far_between_60_to_80 = self.obj.pivot_by_range(data, "percent_80dbm_far",
                                                            ["gateway_macaddress", "polling_date"], 0.8,
                                                            0.60). \
            withColumnRenamed("count", "percent_80dbm_far_between_60_to_80")
        percent_80dbm_far_between_80_to_100 = self.obj.pivot_by_range(data, "percent_80dbm_far",
                                                             ["gateway_macaddress", "polling_date"], 1.0,
                                                             0.80). \
            withColumnRenamed("count", "percent_80dbm_far_between_80_to_100")
        pivot_percent_80dbm_far = self.obj.join_five_frames(percent_80dbm_far_between_80_to_100,
                                                   percent_80dbm_far_between_60_to_80,
                                                   percent_80dbm_far_between_40_to_60,
                                                   percent_80dbm_far_between_20_to_40,
                                                   percent_80dbm_far_between_0_to_20, "outer",
                                                   ["gateway_macaddress", "polling_date"])
        pivot_percent_80dbm_far = pivot_percent_80dbm_far.na.fill(0)

        output = self.obj.join_five_frames(data,
                                  pivot_percent_outlier_far,
                                  pivot_percent_outlier_near,
                                  pivot_percent_80dbm_near,
                                  pivot_percent_80dbm_far, "outer", ["gateway_macaddress", "polling_date"])

        output = output.select("gateway_macaddress",
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
                               "percent_80dbm_far_between_80_to_100"
                               )

        output = output.groupBy("gateway_macaddress", "polling_date").agg(
            func.max("intermittancy_perc").alias("intermittancy_perc"),
            func.max("scaled_accessibility_perc_full_day").alias("scaled_accessibility_perc_full_day"),
            func.max("ticket_count").alias("ticket_count"),
            func.max("percent_outlier_far_between_0_to_20").alias("percent_outlier_far_between_0_to_20"),
            func.max("percent_outlier_far_between_20_to_40").alias("percent_outlier_far_between_20_to_40"),
            func.max("percent_outlier_far_between_40_to_60").alias("percent_outlier_far_between_40_to_60"),
            func.max("percent_outlier_far_between_60_to_80").alias("percent_outlier_far_between_60_to_80"),
            func.max("percent_outlier_far_between_80_to_100").alias("percent_outlier_far_between_80_to_100"),
            func.max("percent_outlier_near_between_0_to_20").alias("percent_outlier_near_between_0_to_20"),
            func.max("percent_outlier_near_between_20_to_40").alias("percent_outlier_near_between_20_to_40"),
            func.max("percent_outlier_near_between_40_to_60").alias("percent_outlier_near_between_40_to_60"),
            func.max("percent_outlier_near_between_60_to_80").alias("percent_outlier_near_between_60_to_80"),
            func.max("percent_outlier_near_between_80_to_100").alias("percent_outlier_near_between_80_to_100"),
            func.max("percent_80dbm_near_between_0_to_20").alias("percent_80dbm_near_between_0_to_20"),
            func.max("percent_80dbm_near_between_20_to_40").alias("percent_80dbm_near_between_20_to_40"),
            func.max("percent_80dbm_near_between_40_to_60").alias("percent_80dbm_near_between_40_to_60"),
            func.max("percent_80dbm_near_between_60_to_80").alias("percent_80dbm_near_between_60_to_80"),
            func.max("percent_80dbm_near_between_80_to_100").alias("percent_80dbm_near_between_80_to_100"),
            func.max("percent_80dbm_far_between_0_to_20").alias("percent_80dbm_far_between_0_to_20"),
            func.max("percent_80dbm_far_between_20_to_40").alias("percent_80dbm_far_between_20_to_40"),
            func.max("percent_80dbm_far_between_40_to_60").alias("percent_80dbm_far_between_40_to_60"),
            func.max("percent_80dbm_far_between_60_to_80").alias("percent_80dbm_far_between_60_to_80"),
            func.max("percent_80dbm_far_between_80_to_100").alias("percent_80dbm_far_between_80_to_100"))

        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_training_pivot")
        output.write.saveAsTable("business_users_test.tmp_single_modem_training_pivot")