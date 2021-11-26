from gw_process.preprocess import preprocessor
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import lit, when
from gw_process.config import config
from pyspark.sql.functions import to_timestamp, date_format


class gw_process:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark=self.con.spark
        self.fiveteen_span = 15
        self.three_span = 7
        self.first_threshold = 0.1
        self.second_threshold = 0.6

    def legacy_wifi_check_gw_aggregations(self, run_date):

        # 1

        connected_devices = self.obj.get_data("pma.net_rog_wifi_associated_devices_fct", ["gateway_macaddress",
                                                                                 "polling_date",
                                                                                 "polling_time",
                                                                                 "associated_device_macaddress",
                                                                                 "associated_device_band",
                                                                                 "signalstrength",
                                                                                 "x_comcast_com_rssi"]). \
            withColumn("current_date", lit(run_date)). \
            withColumn("in_fiveteen_date", func.date_sub(lit(run_date), self.fiveteen_span)). \
            withColumn("in_three_date", func.date_sub(lit(run_date), self.three_span)). \
            withColumn("interface", func.when(col("associated_device_band") == 1, "WiFi 2.4G").otherwise("WiFi 5G")). \
            filter((col("polling_date") <= lit(run_date)) & (col("polling_date") >= func.date_sub(lit(run_date), 0)))


        # hardware_version and modem family
        # 2

        hitron_hardware_versions = self.obj.get_distinct_data("pma.net_rog_wifi_device_dim", ["gateway_macaddress",
                                                                                     "model_family",
                                                                                     "device_deviceinfo_hardwareversion"]). \
            filter(col("model_family") == "HITRON")

        # reading device info
        # 3

        host = self.obj.get_data("pma.net_rog_wifi_host_fct", ["gateway_macaddress",
                                                      "physaddress",
                                                      "polling_date",
                                                      "hostname"]). \
            withColumnRenamed("physaddress", "associated_device_macaddress"). \
            withColumnRenamed("hostname", "devicename"). \
            filter((col("polling_date") <= lit(run_date)) & (col("polling_date") >= func.date_sub(lit(run_date), 0)))



        # 4
        host_name_not_unknown = host.filter((col("devicename") != 'Unknown') & (col("devicename").isNotNull())
                                            & (col("devicename") != 'unknown')). \
            filter("devicename NOT like '%:%'"). \
            groupBy(["gateway_macaddress", "associated_device_macaddress"]).agg({"devicename": "max"}). \
            withColumnRenamed("max(devicename)", "max(devicename)")


        # 5
        host = host.select("gateway_macaddress",
                           "associated_device_macaddress",
                           "polling_date")

        # 6
        host_name = self.obj.join_two_frames(host, host_name_not_unknown, "left",
                                    ["gateway_macaddress", "associated_device_macaddress"]). \
            withColumnRenamed("max(devicename)", "devicename")

        # joining hardware versions with connected device data
        # 7
        connected_devices_hitron = self.obj.join_two_frames(connected_devices,
                                                   hitron_hardware_versions,
                                                   "inner",
                                                   "gateway_macaddress"). \
            withColumn("signalstrength", func.when((col("device_deviceinfo_hardwareversion") == 'CODA-4582-1A') |
                                                   (col("device_deviceinfo_hardwareversion") == 'CODA-4582-2A'),
                                                   func.when((col("x_comcast_com_rssi").isNotNull()) &
                                                             (col("x_comcast_com_rssi") != -100) &
                                                             (col("x_comcast_com_rssi") != 0),
                                                             col("x_comcast_com_rssi")). \
                                                   otherwise(func.when((col("signalstrength").isNotNull()) &
                                                                       (col("signalstrength") != -100) &
                                                                       (col("signalstrength") != 0),
                                                                       col("signalstrength")). \
                                                             otherwise(col("signalstrength")))). \
                       otherwise(func.when((col("signalstrength").isNotNull()) &
                                           (col("signalstrength") != -100) &
                                           (col("signalstrength") != 0), col("signalstrength")). \
                                 otherwise(col("x_comcast_com_rssi"))))

        # 8
        connected_devices_hitron = connected_devices_hitron.select("gateway_macaddress",
                                                                   "polling_date",
                                                                   "polling_time",
                                                                   "interface",
                                                                   "associated_device_macaddress",
                                                                   "signalstrength",
                                                                   "current_date",
                                                                   "in_fiveteen_date",
                                                                   "in_three_date",
                                                                   "model_family",
                                                                   "device_deviceinfo_hardwareversion")

        # joining device info with connected device data

        connected_devices_hitron_host_single = self.obj.join_two_frames(connected_devices_hitron,
                                                        host_name,
                                                        "left",
                                                        ["gateway_macaddress",
                                                         "associated_device_macaddress",
                                                         "polling_date"]). \
            filter((col("polling_date") <= lit(run_date)) & (col("polling_date") >= func.date_sub(lit(run_date), 0)))


        connected_devices_hitron_host_hist = self.obj.get_data("lwc.tmp_single_modems_historical_run","*")

        connected_devices_hitron_host = self.obj.unionAll(*[connected_devices_hitron_host_single, connected_devices_hitron_host_hist])

        # Check the number of polls in the last 15 days
        # 9
        filter_based_on_last_fiveteen_days = self.obj.filter_records(connected_devices_hitron_host,
                                                            ["gateway_macaddress", "polling_date"],
                                                            "in_fiveteen_date",
                                                            "distinct_count_fiveteen",
                                                            "percent_second_threshold_GW",
                                                            self.fiveteen_span,
                                                            "gateway_macaddress")

        filter_based_on_last_three_days = self.obj.filter_records(connected_devices_hitron_host,
                                                         ["gateway_macaddress", "polling_date"],
                                                         "in_three_date",
                                                         "distinct_count_three",
                                                         "percent_first_threshold_GW",
                                                         self.three_span,
                                                         "gateway_macaddress")

        # joining both filters based on 15 and 3 days
        # 10
        connection_joined_both = self.obj.join_two_frames(filter_based_on_last_three_days,
                                                 filter_based_on_last_fiveteen_days,
                                                 "left",
                                                 "gateway_macaddress")

        # joining filters by 15 and 3 days to connected device data

        connection_joined_fiveteen_three = self.obj.join_two_frames(connected_devices_hitron_host,
                                                           connection_joined_both,
                                                           "left",
                                                           "gateway_macaddress")
        # tagging gateway's based on threshold
        # 11
        connection_joined_fiveteen_three_new_higher = connection_joined_fiveteen_three. \
            filter(
            ((connection_joined_fiveteen_three["percent_second_threshold_GW"].cast(
                DecimalType()) >= self.second_threshold)) | (
                        connection_joined_fiveteen_three["percent_first_threshold_GW"].cast(
                            DecimalType()) >= self.first_threshold)). \
            orderBy(["polling_date"], ascending=[0]).withColumn("missing_data", lit("No"))
        # tagging gateway's based on threshold

        connection_joined_fiveteen_three_new_lesser = connection_joined_fiveteen_three. \
            filter(
            ((connection_joined_fiveteen_three["percent_second_threshold_GW"].cast(
                DecimalType()) < self.second_threshold)) | (
                        connection_joined_fiveteen_three["percent_first_threshold_GW"].cast(
                            DecimalType()) < self.first_threshold)). \
            orderBy(["polling_date"], ascending=[0]).withColumn("missing_data", lit("Yes"))
        # joining above two filters

        connection_joined_fiveteen_three_new = connection_joined_fiveteen_three_new_higher. \
            union(connection_joined_fiveteen_three_new_lesser)

        # 12
        connection_joined_fiveteen_three_new = connection_joined_fiveteen_three_new.withColumn("polling_timestamp",
                                                                                               date_format(to_timestamp(
                                                                                                   col("polling_time"),
                                                                                                   "hh:mm:ss"),
                                                                                                   'hh:mm:ss')). \
            withColumn("polling_span", func.when((col("polling_timestamp") >=
                                                  date_format(to_timestamp(lit("00:00:00"), "hh:mm:ss"), 'hh:mm:ss'))
                                                 | (col("polling_timestamp") <= date_format(
            to_timestamp(lit("08:00:00"),
                         "hh:mm:ss"),
            'hh:mm:ss'))
                                                 , "Overnight").otherwise("Daylight"))
        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem")

        # replacing null with unknown

        connection_joined_fiveteen_three_new = connection_joined_fiveteen_three_new.na.fill("Unknown"). \
            withColumnRenamed("device_deviceinfo_hardwareversion", "hardwareversion")


        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modems_v1")

        self.spark.sql("CREATE TABLE IF NOT EXISTS lwc.tmp_single_modems_v1(gateway_macaddress string,associated_device_macaddress string, interface string,polling_date string,polling_time string,polling_span string,signalstrength string,devicename string,model_family string,hardwareversion string,missing_data string,percent_first_threshold_GW double,percent_second_threshold_GW double)")

        connection_joined_fiveteen_three_new.select("gateway_macaddress",
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
                                                "percent_second_threshold_GW"
                                                ).\
            write.saveAsTable("lwc.tmp_single_modem")

        self.spark.sql("INSERT INTO TABLE lwc.tmp_single_modems_v1 SELECT * from lwc.tmp_single_modem")

        return True
