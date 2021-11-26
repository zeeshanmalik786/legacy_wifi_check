from algorithm.preprocess import preprocessor
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col
from algorithm.config import config


class means:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def mean_one(self):

        self.spark.sql("REFRESH TABLE lwc.tmp_single_modem_device")
        data = self.obj.get_data("lwc.tmp_single_modem_device", ["gateway_macaddress",
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
                                                                        "percent_second_threshold_GW"]). \
            filter(col("missing_data_device") == "No")

        mean_df = data.groupBy("associated_device_macaddress"). \
            agg(func.mean("signalstrength").
                alias("mean_signalstrength"))

        mean_join = self.obj.join_two_frames(data, mean_df, "inner", "associated_device_macaddress")

        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_device_means")

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
                         "mean_signalstrength"
                         ).write.saveAsTable("lwc.tmp_single_modem_device_means")

        return True