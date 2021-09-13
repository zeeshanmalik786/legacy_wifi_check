from preprocessor.preprocess import preprocessor
from pyspark.sql import functions as func
from config.config import config


class mean_absolute_deviation():

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def mean_absolute_deviation(self):
        data = self.obj.get_data("business_users_test.tmp_single_modem_device_absolute_deviation", ["gateway_macaddress",
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
                                                                                           "absdev"])

        mean_abs_df = data.groupBy("associated_device_macaddress"). \
             agg(func.mean("absdev").alias("mean_absolute_deviation"))

        output = self.obj.join_two_frames(data,mean_abs_df,"inner", "associated_device_macaddress")

        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_device_mean_absolute_deviation")

        output.select("gateway_macaddress",
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
                  "mean_absolute_deviation").write.saveAsTable("business_users_test.tmp_single_modem_device_mean_absolute_deviation")

        return True