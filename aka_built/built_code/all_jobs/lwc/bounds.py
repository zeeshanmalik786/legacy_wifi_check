from algorithm.preprocess import preprocessor
from pyspark.sql.functions import rank, col
from algorithm.config import config


class bounds:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def bounds(self):


        data = self.obj.get_data("lwc.tmp_single_modem_device_mean_absolute_deviation",
                        ["gateway_macaddress",
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
                         "percent_second_threshold_Device",
                         "percent_first_threshold_Device",
                         "percent_first_threshold_GW",
                         "percent_second_threshold_GW",
                         "mean_signalstrength",
                         "absdev",
                         "mean_absolute_deviation"])

        mean_bounds = data. \
            withColumn("upperbound", col("mean_signalstrength") + col("mean_absolute_deviation")). \
            withColumn("lowerbound", col("mean_signalstrength") - col("mean_absolute_deviation"))

        mean_bounds.select("gateway_macaddress",
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
                           "percent_second_threshold_Device",
                           "percent_first_threshold_Device",
                           "percent_first_threshold_GW",
                           "percent_second_threshold_GW",
                           "mean_signalstrength",
                           "absdev",
                           "mean_absolute_deviation",
                           "lowerbound",
                           "upperbound")

        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_device_bounder")
        mean_bounds.write.saveAsTable("lwc.tmp_single_modem_device_bounder")

        return True