from gw_ethernet.preprocess import preprocessor
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col
from pyspark.sql.functions import lit, when
from gw_ethernet.config import config
from pyspark.sql.functions import to_timestamp, date_format


class gw_ethernet:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def ethernet(self, run_date):
        # 1
        hitron_ethernet = self.obj.get_data("pma.net_rog_wifi_host_fct", ["gateway_macaddress",
                                                                 "physaddress",
                                                                 "polling_date",
                                                                 "polling_time",
                                                                 "hostname",
                                                                 "layer1interface"]). \
            withColumnRenamed("physaddress", "associated_device_macaddress"). \
            withColumnRenamed("hostname", "devicename"). \
            filter(col("polling_date") == run_date). \
            filter(col("layer1interface").like("%Ethernet%")). \
            withColumnRenamed("layer1interface", "interface"). \
            withColumnRenamed("physaddress", "associated_device_macaddress"). \
            withColumn("signalstrength", lit("None")). \
            withColumn("interface", lit("Ethernet")). \
            withColumnRenamed("hostname", "devicename"). \
            withColumn("missing_data", lit("No")). \
            withColumn("percent_first_threshold_GW", lit(1)). \
            withColumn("percent_second_threshold_GW", lit(1))

        # 2
        hitron_hardware_versions = self.obj.get_distinct_data("pma.net_rog_wifi_device_dim", ["gateway_macaddress",
                                                                                     "model_family",
                                                                                     "device_deviceinfo_hardwareversion"]). \
            filter(col("model_family") == "HITRON")


        # 3
        hitron_ethernet_hardware = self.obj.join_two_frames(hitron_ethernet,
                                                   hitron_hardware_versions,
                                                   "inner",
                                                   ["gateway_macaddress"]). \
            withColumnRenamed("device_deviceinfo_hardwareversion", "hardwareversion")


        # 4
        hitron_ethernet_hardware = hitron_ethernet_hardware.withColumn("polling_timestamp",
                                                                       date_format(
                                                                           to_timestamp(col("polling_time"),
                                                                                        "hh:mm:ss"),
                                                                           'hh:mm:ss')). \
            withColumn("polling_span", func.when((col("polling_timestamp") >=
                                                  date_format(to_timestamp(lit("00:00:00"), "hh:mm:ss"), 'hh:mm:ss'))
                                                 | (col("polling_timestamp") <= date_format(
            to_timestamp(lit("08:00:00"),
                         "hh:mm:ss"),
            'hh:mm:ss'))
                                                 , "Overnight").otherwise("Daylight"))
        # 5
        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_ethernet")

        # 6
        hitron_ethernet_hardware = hitron_ethernet_hardware.select("gateway_macaddress",
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
                                                                   "percent_second_threshold_GW"). \
            filter(col("polling_date") <= run_date)
        connection_joined_fiveteen_three_new = self.obj.get_data("lwc.tmp_single_modems_v1",
                                                        ["gateway_macaddress",
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
                                                         "percent_second_threshold_GW"])

        output = connection_joined_fiveteen_three_new.union(hitron_ethernet_hardware)
        output.write.saveAsTable("lwc.tmp_single_modem_ethernet")

        return True