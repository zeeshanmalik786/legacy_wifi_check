from unified_ssid.preprocess import preprocessor
from pyspark.sql.functions import rank, col
from unified_ssid.config import config
from pyspark.sql.functions import countDistinct
from pyspark.sql import functions as func


class unify:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def unified_ssid(self, run_date):

        # 1
        df_ssid = self.obj.get_data("pma.net_rog_wifi_ssid_fct", ["gateway_macaddress",
                                                         "ssid",
                                                         "polling_date"])

        # 2
        df_modem_device = self.obj.get_data("lwc.tmp_single_modem_ethernet", ["gateway_macaddress",
                                                                                     "associated_device_macaddress",
                                                                                     "interface",
                                                                                     "polling_date",
                                                                                     "polling_time",
                                                                                     "polling_span",
                                                                                     "signalstrength",
                                                                                     "devicename",
                                                                                     "model_family",
                                                                                     "hardwareversion",
                                                                                     "missing_data",
                                                                                     "percent_first_threshold_GW",
                                                                                     "percent_second_threshold_GW"]). \
            filter(col("polling_date") <= run_date)
        # Change for Production filter(col("polling_date") < run_date


        # max_date = df_ssid.agg({"polling_date": "max"}).collect()[0][0]
        #
        # filter_records = df_ssid. \
        #     filter(col("polling_date") == max_date)
        # 3
        agg_ssid = df_ssid.groupBy("gateway_macaddress", "polling_date").agg(countDistinct("ssid")). \
            withColumnRenamed("count(ssid)", "count_of_ssid"). \
            withColumn("unified_ssid", func.when(col("count_of_ssid") == 1, "Yes").otherwise("No"))

        #change for production

        # agg_ssid = df_ssid.groupBy("gateway_macaddress", "polling_date").agg(countDistinct("ssid")). \
        #     withColumnRenamed("count(DISTINCT ssid)", "count_of_ssid"). \
        #     withColumn("unified_ssid", func.when(col("count_of_ssid") == 1, "Yes").otherwise("No"))

        # hitron_hardware_versions = self.obj.get_distinct_data("pma.net_rog_wifi_device_dim", ["gateway_macaddress",
        #                                                                                       "model_family",
        #                                                                                       "device_deviceinfo_hardwareversion"]). \
        #     filter(col("model_family") == "HITRON").select("gateway_macaddress")
        #
        # filtered_by_hardware = self.obj.join_two_frames(agg_ssid, hitron_hardware_versions, "left", "gateway_macaddress")
        # 4
        combined = self.obj.join_two_frames(df_modem_device, agg_ssid, "left", ["gateway_macaddress", "polling_date"])
        # 5
        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_unify_ssid")
        # 6
        combined.select("gateway_macaddress",
                        "associated_device_macaddress",
                        "interface",
                        "unified_ssid",
                        "polling_date",
                        "polling_time",
                        "polling_span",
                        "signalstrength",
                        "devicename",
                        "model_family",
                        "hardwareversion",
                        "missing_data",
                        "percent_first_threshold_GW",
                        "percent_second_threshold_GW").write. \
            saveAsTable("lwc.tmp_single_modem_unify_ssid")

        return True





















