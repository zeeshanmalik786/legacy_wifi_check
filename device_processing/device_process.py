from preprocessor.preprocess import preprocessor
from pyspark.sql import functions as func
from pyspark.sql.functions import countDistinct, count
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit
from config.config import config


class device_process:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.ten_span = 30
        self.three_span = 7
        self.filteration_threshold_small = 0.8
        self.filteration_threshold_big=0.4
        self.sqlcontext = self.con.context


    def legacy_wifi_check_device_aggregations(self, run_date):

        # Note: Here Removing Devices that has less telemetry based on defined thresholds

        processed_data = self.obj.get_data("business_users_test.tmp_single_modem_unify_ssid",["gateway_macaddress",
                                                                                 "associated_device_macaddress",
                                                                                 "polling_date",
                                                                                 "polling_time",
                                                                                 "polling_span",
                                                                                 "signalstrength",
                                                                                 "devicename",
                                                                                 "interface",
                                                                                 "unified_ssid",
                                                                                 "model_family",
                                                                                 "hardwareversion",
                                                                                 "missing_data",
                                                                                 "percent_first_threshold_GW",
                                                                                 "percent_second_threshold_GW"]).\
        withColumn("current_date", lit(run_date)). \
        withColumn("in_ten_date", func.date_sub(lit(run_date), self.ten_span)). \
        withColumn("in_three_date", func.date_sub(lit(run_date), self.three_span))

        # Filtering out Temporary Devices

        # Device Activeness End to End

        min_max = self.obj.device_activeness(processed_data, ["gateway_macaddress",
                                       "associated_device_macaddress",
                                       "polling_date"],"gateway_macaddress",
                                "polling_date",
                                self.obj,
                                "delta_end_to_end")

        distinct_devices_per_day = processed_data.\
        select("gateway_macaddress", "associated_device_macaddress").\
        groupBy("gateway_macaddress", "associated_device_macaddress").\
        agg(count("associated_device_macaddress").\
        alias("count_per_day_all")).\
        orderBy("count_per_day_all", ascending=[0])

        device_aggregation = self.obj.join_two_frames(distinct_devices_per_day, min_max,
                                                    "inner", "gateway_macaddress")

        device_aggregation = device_aggregation.withColumn("device_activeness_end_to_end",
                                            func.col("count_per_day_all") / func.col("delta_end_to_end"))

        # Device Activeness Last ten days

        filter_based_on_last_ten_days = preprocessor.filter_records(processed_data,
                                                                ["gateway_macaddress",
                                                                 "associated_device_macaddress",
                                                                 "polling_date"],
                                                                  "in_ten_date",
                                                                  "distinct_count_ten",
                                                                  "percent_ten_days",
                                                                  self.ten_span,
                                                                ["gateway_macaddress",
                                                                 "associated_device_macaddress"])

        # Device Activeness Last three days

        filter_based_on_last_three_days = preprocessor.filter_records(processed_data,
                                                                ["gateway_macaddress",
                                                                 "associated_device_macaddress",
                                                                 "polling_date"],
                                                                "in_three_date",
                                                                "distinct_count_three",
                                                                "percent_three_days",
                                                                self.three_span,
                                                                ["gateway_macaddress",
                                                                 "associated_device_macaddress"])

        join_filters = preprocessor.join_two_frames(filter_based_on_last_ten_days, filter_based_on_last_three_days, "inner",
                                                 ["gateway_macaddress", "associated_device_macaddress"])

        device_aggregation = preprocessor.join_two_frames(device_aggregation, join_filters,
                                                      "inner", ["gateway_macaddress", "associated_device_macaddress"])

        device_aggregation_No = device_aggregation.\
        filter((device_aggregation["percent_ten_days"] >= self.filteration_threshold_small)).\
        withColumn("missing_data_device", lit("No"))

        device_aggregation_Yes = device_aggregation. \
        filter((device_aggregation["percent_ten_days"] < self.filteration_threshold_small)).\
        withColumn("missing_data_device", lit("Yes"))

        device_aggregation_final = device_aggregation_No.union(device_aggregation_Yes)

        device_aggregation_new = device_aggregation_final.withColumn("percent_three_days",
                                                       func.when(col("percent_three_days") > 1, 1).\
                                                       otherwise(col("percent_three_days"))).\
        withColumn("percent_ten_days", func.when(col("percent_ten_days") > 1, 1).\
                   otherwise(col("percent_ten_days")))

        selected_devices = preprocessor.join_two_frames(processed_data, device_aggregation_new, "left",
                                                    ["gateway_macaddress", "associated_device_macaddress"])

        selected_devices = selected_devices.withColumn("signalstrength", func.when(col("signalstrength") > 0, 0).\
                                                   otherwise(col("signalstrength")))

        selected_devices = selected_devices.withColumn("signalstrength", func.when(col("signalstrength") < -100, -100).\
                                                   otherwise(col("signalstrength")))

        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_device")

        selected_devices.select("gateway_macaddress",
                            "associated_device_macaddress",
                            "interface",
                            "unified_ssid",
                            "hardwareversion",
                            "signalstrength",
                            "polling_date",
                            "polling_time",
                            "polling_span",
                            "devicename",
                            "missing_data",
                            "missing_data_device",
                            "percent_first_threshold_GW",
                            "percent_second_threshold_GW",
                            "percent_ten_days",
                            "percent_three_days").\
        withColumnRenamed("percent_ten_days","percent_first_threshold_Device").\
        withColumnRenamed("percent_three_days","percent_second_threshold_Device").\
        write.saveAsTable("business_users_test.tmp_single_modem_device")

        return True