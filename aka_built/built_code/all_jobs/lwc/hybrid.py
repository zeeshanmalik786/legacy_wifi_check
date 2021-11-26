from algorithm.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit, abs
from algorithm.config import config
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col, concat
from pyspark.sql.functions import current_date


class hybrid:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

        self.aggregation_window_far  = 120
        self.aggregation_window_near = 30
        self.distributor=0
        self.weights_percent_outlier_near = 0.25
        self.weights_percent_outlier_far = 0.25
        self.weights_percent_80dbm_far = 0.25
        self.weights_percent_80dbm_near = 0.25
        self.number_of_features=4

    def hybrid(self,run_date):
        first = self.obj.get_data("lwc.tmp_single_modem_device_bounder", ["gateway_macaddress",
                                                                                 "associated_device_macaddress",
                                                                                 "interface",
                                                                                 "unified_ssid",
                                                                                 "signalstrength",
                                                                                 "polling_date",
                                                                                 "polling_time",
                                                                                 "polling_span",
                                                                                 "devicename",
                                                                                 "hardwareversion",
                                                                                 "missing_data",
                                                                                 "missing_data_device",
                                                                                 "lowerbound",
                                                                                 "upperbound"]). \
            withColumn("current_date", lit(run_date)). \
            withColumn("aggregation_date", func.date_sub(lit(run_date), self.aggregation_window_far)). \
            withColumn("aggregation_data_near", func.date_sub(lit(run_date), self.aggregation_window_near))

        second = self.obj.get_data("lwc.tmp_single_modem_device", ["gateway_macaddress",
                                                                          "associated_device_macaddress",
                                                                          "interface",
                                                                          "unified_ssid",
                                                                          "signalstrength",
                                                                          "polling_date",
                                                                          "polling_time",
                                                                          "polling_span",
                                                                          "devicename",
                                                                          "hardwareversion",
                                                                          "missing_data",
                                                                          "missing_data_device"]). \
            withColumn("upperbound", lit(0)). \
            withColumn("lowerbound", lit(0)). \
            withColumn("outlier", lit(0)). \
            withColumn("percent_outlier_near", lit(0)). \
            withColumn("percent_outlier_far", lit(0)). \
            withColumn("percent_80dbm_near", lit(0)). \
            withColumn("percent_80dbm_far", lit(0)). \
            withColumn("less_than_80dbm", lit(0)). \
            filter(col("missing_data_device") == "Yes")

        magic_percentile = func.expr('percentile_approx(signalstrength, 0.5)')

        first = first.groupBy("gateway_macaddress",
                              "associated_device_macaddress",
                              "interface",
                              "unified_ssid",
                              "polling_date",
                              "polling_span",
                              "devicename",
                              "hardwareversion",
                              "missing_data",
                              "missing_data_device",
                              "lowerbound",
                              "upperbound",
                              "current_date",
                              "aggregation_date",
                              "aggregation_data_near").agg(magic_percentile.alias('signalstrength')). \
            withColumn("outlier", func.when(col("signalstrength") <= col("lowerbound"), 1).otherwise(0)). \
            withColumn("less_than_80dbm", func.when(col('signalstrength') <= -80, 1).otherwise(0))

        frame1 = first.where((first.polling_date >= first.aggregation_date) &
                             (first.polling_date <= first.current_date))

        frame1 = frame1.select("gateway_macaddress", "associated_device_macaddress", "polling_date", "outlier",
                               "less_than_80dbm"). \
            groupBy(["gateway_macaddress", "associated_device_macaddress"]).sum("outlier", "less_than_80dbm"). \
            withColumnRenamed("sum(outlier)", "sum_of_outlier_far"). \
            withColumnRenamed("sum(less_than_80dbm)", "sum_of_less_80dbm"). \
            withColumn("percent_outlier_far",
                       func.when((func.col("sum_of_outlier_far") / self.aggregation_window_far) >= 1, 1). \
                       otherwise((func.col("sum_of_outlier_far") / self.aggregation_window_far))). \
            withColumn("percent_80dbm_far", func.when((func.col("sum_of_less_80dbm") / self.aggregation_window_far) >= 1, 1). \
                       otherwise((func.col("sum_of_less_80dbm") / self.aggregation_window_far)))

        frame2 = first.where((first.polling_date >= first.aggregation_data_near) &
                             (first.polling_date <= first.current_date))

        frame2 = frame2.select("gateway_macaddress", "associated_device_macaddress", "polling_date", "outlier",
                               "less_than_80dbm"). \
            groupBy(["gateway_macaddress", "associated_device_macaddress"]).sum("outlier", "less_than_80dbm"). \
            withColumnRenamed("sum(outlier)", "sum_of_outlier_near"). \
            withColumnRenamed("sum(less_than_80dbm)", "sum_of_less_80dbm"). \
            withColumn("percent_outlier_near",
                       func.when((func.col("sum_of_outlier_near") / self.aggregation_window_near) >= 1, 1). \
                       otherwise((func.col("sum_of_outlier_near") / self.aggregation_window_near))). \
            withColumn("percent_80dbm_near",
                       func.when((func.col("sum_of_less_80dbm") / self.aggregation_window_near) >= 1, 1). \
                       otherwise((func.col("sum_of_less_80dbm") / self.aggregation_window_near)))

        frame_1_frame_2 = self.obj.join_two_frames(frame1, frame2, "inner", ["gateway_macaddress",
                                                                    "associated_device_macaddress"])

        first = first.select("gateway_macaddress",
                             "associated_device_macaddress",
                             "interface",
                             "unified_ssid",
                             "signalstrength",
                             "polling_date",
                             "polling_span",
                             "devicename",
                             "hardwareversion",
                             "missing_data",
                             "missing_data_device",
                             "lowerbound",
                             "upperbound",
                             "outlier",
                             "less_than_80dbm")

        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modems_v1")

        frame_1_frame_2_tag = frame_1_frame_2.select("gateway_macaddress",
                                                     "associated_device_macaddress",
                                                     "percent_outlier_near",
                                                     "percent_outlier_far",
                                                     "percent_80dbm_near",
                                                     "percent_80dbm_far")

        join_first_frame_1_frame_2 = self.obj.join_two_frames(frame_1_frame_2_tag,
                                                     first, "inner", ["gateway_macaddress","associated_device_macaddress"])

        join_first_frame_1_frame_2 = join_first_frame_1_frame_2.select("gateway_macaddress",
                                                                       "associated_device_macaddress",
                                                                       "interface",
                                                                       "unified_ssid",
                                                                       "percent_outlier_near",
                                                                       "percent_outlier_far",
                                                                       "percent_80dbm_near",
                                                                       "percent_80dbm_far",
                                                                       "signalstrength",
                                                                       "polling_date",
                                                                       "polling_span",
                                                                       "devicename",
                                                                       "hardwareversion",
                                                                       "missing_data",
                                                                       "missing_data_device",
                                                                       "lowerbound",
                                                                       "upperbound",
                                                                       "outlier",
                                                                       "less_than_80dbm"). \
            withColumn("norm_signalstrength", (func.when(((col("signalstrength") - (-60)) / ((-80) - (-60))) > 1, 1).
                                               otherwise(
            func.when(((col("signalstrength") - (-60)) / ((-80) - (-60))) < 0, 0).
            otherwise(((col("signalstrength") - (-60)) / ((-80) - (-60)))))))

        second = second.select("gateway_macaddress",
                               "associated_device_macaddress",
                               "interface",
                               "unified_ssid",
                               "percent_outlier_near",
                               "percent_outlier_far",
                               "percent_80dbm_near",
                               "percent_80dbm_far",
                               "signalstrength",
                               "polling_date",
                               "polling_span",
                               "devicename",
                               "hardwareversion",
                               "missing_data",
                               "missing_data_device",
                               "lowerbound",
                               "upperbound",
                               "outlier",
                               "less_than_80dbm"). \
            withColumn("norm_signalstrength", (func.when(((col("signalstrength") - (-60)) / ((-80) - (-60))) > 1, 1).
                                               otherwise(
            func.when(((col("signalstrength") - (-60)) / ((-80) - (-60))) < 0, 0).
            otherwise(((col("signalstrength") - (-60)) / ((-80) - (-50)))))))

        magic_percentile = func.expr('percentile_approx(norm_signalstrength, 0.5)')

        second_agg = second.groupBy("gateway_macaddress",
                                    "associated_device_macaddress"
                                    ).agg(magic_percentile.alias('mednorm_signalstrength'))

        second_joined = self.obj.join_two_frames(second, second_agg, "inner",
                                        ["gateway_macaddress", "associated_device_macaddress"])

        second_joined = second_joined.select("gateway_macaddress",
                                             "associated_device_macaddress",
                                             "interface",
                                             "unified_ssid",
                                             "percent_outlier_near",
                                             "percent_outlier_far",
                                             "percent_80dbm_near",
                                             "percent_80dbm_far",
                                             "signalstrength",
                                             "polling_date",
                                             "polling_span",
                                             "devicename",
                                             "hardwareversion",
                                             "missing_data",
                                             "missing_data_device",
                                             "lowerbound",
                                             "upperbound",
                                             "outlier",
                                             "less_than_80dbm",
                                             "mednorm_signalstrength").withColumnRenamed("mednorm_signalstrength",
                                                                                         "norm_signalstrength")

        output = second_joined.union(join_first_frame_1_frame_2). \
            withColumn("norm_signalstrength", func.when(col("norm_signalstrength") < 0, 0).
                       otherwise(func.when(col("norm_signalstrength") > 1, 1).
                                 otherwise(col("norm_signalstrength")))). \
            withColumnRenamed("lowerbound", "predicted_lowerbound"). \
            withColumnRenamed("upperbound", "predicted_upperbound"). \
            withColumn("distributor_percent_outlier_near", func.when(col("percent_outlier_near") == 0, (
                    self.weights_percent_outlier_near / self.number_of_features)).otherwise(0)). \
            withColumn("distributor_percent_outlier_far", func.when(col("percent_outlier_far") == 0, (
                    self.weights_percent_outlier_far / self.number_of_features)).otherwise(0)). \
            withColumn("distributor_percent_80dbm_near", func.when(col("percent_80dbm_far") == 0, (
                    self.weights_percent_80dbm_far / self.number_of_features)).otherwise(0)). \
            withColumn("distributor_percent_80dbm_far", func.when(col("percent_80dbm_near") == 0, (
                    self.weights_percent_80dbm_near / self.number_of_features)).otherwise(0))

        output = output.withColumn("device_score",
                                   func.when((col("missing_data_device") == "Yes") & (col("interface") != "Ethernet"),
                                             func.round((100 * (1 - (col("norm_signalstrength") * 1))))). \
                                   otherwise(func.when(col("interface") == "Ethernet", "Not Applicable"). \
                                             otherwise(func.round((100 * (1 - ((col("percent_outlier_near") * (
                                               self.weights_percent_outlier_near + col("distributor_percent_outlier_near") +
                                               col("distributor_percent_outlier_far") +
                                               col("distributor_percent_80dbm_near") +
                                               col("distributor_percent_80dbm_far")))
                                                                               +
                                                                               (col("percent_outlier_far") * (
                                                                                           self.weights_percent_outlier_far + col(
                                                                                       "distributor_percent_outlier_near") +
                                                                                           col(
                                                                                               "distributor_percent_outlier_far") +
                                                                                           col(
                                                                                               "distributor_percent_80dbm_near") +
                                                                                           col(
                                                                                               "distributor_percent_80dbm_far")))
                                                                               +
                                                                               (col("percent_80dbm_far") * (
                                                                                           self.weights_percent_80dbm_far + col(
                                                                                       "distributor_percent_outlier_near") +
                                                                                           col(
                                                                                               "distributor_percent_outlier_far") +
                                                                                           col(
                                                                                               "distributor_percent_80dbm_near") +
                                                                                           col(
                                                                                               "distributor_percent_80dbm_far")))
                                                                               +
                                                                               (col("percent_80dbm_near") * (
                                                                                           self.weights_percent_80dbm_near + col(
                                                                                       "distributor_percent_outlier_near") +
                                                                                           col(
                                                                                               "distributor_percent_outlier_far") +
                                                                                           col(
                                                                                               "distributor_percent_80dbm_near") +
                                                                                           col(
                                                                                               "distributor_percent_80dbm_far")))))),
                                                                  2).alias("device_score"))))
        output = output.withColumn("device_score", func.when(((col("percent_outlier_near") == 0) &
                                                              (col("percent_outlier_far") == 0) &
                                                              (col("percent_80dbm_near") == 0) &
                                                              (col("percent_80dbm_far") == 0)),
                                                             ((100 - (col("norm_signalstrength") * 100)))).otherwise(col("device_score")))

        output = output. \
            withColumn("Rating", func.when((col("device_score") <= 30), "Intermittent Device").
                       otherwise(func.when((col("device_score") > 30) & (col("device_score") <= 40), "Bad Device").
                                 otherwise(
            func.when((col("device_score") <= 80) & (col("device_score") > 40), "Good Device").
            otherwise(func.when((col("device_score") > 80), "Excellent Device")))))
        output = output. \
            withColumn("device_score", func.when(col("interface") == "WiFi 2.4G", func.when(col("device_score") >= 10,
                                                                                            col(
                                                                                                "device_score") - 10).otherwise(
            col("device_score"))).otherwise(col("device_score")))

        output.withColumn("Rating",
                          func.when(col("device_score") == "Not Applicable", "None").otherwise(col("Rating"))). \
            write.saveAsTable("lwc.tmp_single_modems_v1")

        return True