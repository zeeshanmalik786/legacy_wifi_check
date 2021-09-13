from preprocessor.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit, abs
from config.config import config


class absolute_deviation:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark


    def absolute_deviation(self):

        data = self.obj.get_data("business_users_test.tmp_single_modem_device_means", ["gateway_macaddress",
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
                                                                             "mean_signalstrength"])

        mean_join = data.withColumn("absdev",
                                     abs(col("signalstrength") - col("mean_signalstrength")))

        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_device_absolute_deviation")

        mean_join.select("gateway_macaddress",
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
                     "absdev"
                     ).write.saveAsTable("business_users_test.tmp_single_modem_device_absolute_deviation")

        return True