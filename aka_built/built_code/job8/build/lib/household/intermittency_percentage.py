from household.preprocess import preprocessor
from household.config import config
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col, concat, upper
from pyspark.sql.functions import lit
from pyspark.sql.functions import regexp_replace

# Count of Calls Near
# Count of Calls Far
# Intermittency Percentage
# Buckets of Device Score
# Buckets of Predicted Thresholds


# Issue 1 : The current date issue still needs to be resolved.
# Issue 2 : Count is decreasing at this stage

class intermittency_percentage:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def households(self, run_date):


        self.spark.sql("REFRESH TABLE lwc.tmp_single_modem_additional")
        data = self.obj.get_data("lwc.tmp_single_modem_additional", ["gateway_macaddress",
                                                                      "associated_device_macaddress",
                                                                      "interface",
                                                                      "unified_ssid",
                                                                      "percent_outlier_far",
                                                                      "percent_outlier_near",
                                                                      "percent_80dbm_near",
                                                                      "percent_80dbm_far",
                                                                      "signalstrength",
                                                                      "polling_date",
                                                                      "polling_span",
                                                                      "devicename",
                                                                      "hardwareversion",
                                                                      "missing_data",
                                                                      "missing_data_device",
                                                                      "predicted_lowerbound",
                                                                      "predicted_upperbound",
                                                                      "outlier",
                                                                      "less_than_80dbm",
                                                                      "norm_signalstrength",
                                                                      "device_score",
                                                                      "Rating",
                                                                      "CASE_ID",
                                                                      "CASE_NUMBER",
                                                                      "CASE_TYPE_LVL1",
                                                                      "CASE_TYPE_LVL2",
                                                                      "CASE_TYPE_LVL3",
                                                                      "CASE_TYPE_LVL4",
                                                                      "CASE_TYPE_LVL5",
                                                                      "CASE_STATUS"]). \
            withColumn("min_date", func.date_sub(lit(run_date), 120))

        accessibility_daily = self.obj.get_data("hem.modem_accessibility_daily", ["mac",
                                                                         "accessibility_perc_full_day",
                                                                         "event_date"]). \
            withColumn("gateway_macaddress", upper(regexp_replace("mac", "-", ""))).\
            withColumnRenamed("event_date", "polling_date")

        # filter last 30 days of record

        filter_data = data.filter((col("polling_date") >= col("min_date")))

        # filter_data = filter_data.filter((col("missing_data_device") == "No"))

        filter_data_accessibility = self.obj.join_two_frames(filter_data, accessibility_daily, "left",
                                                    ["gateway_macaddress", "polling_date"])

        filter_data_accessibility = filter_data_accessibility.select("gateway_macaddress",
                                                                     "associated_device_macaddress",
                                                                     "polling_date",
                                                                     "device_score",
                                                                     "interface",
                                                                     "accessibility_perc_full_day",
                                                                     "percent_outlier_far",
                                                                     "percent_outlier_near",
                                                                     "percent_80dbm_near",
                                                                     "percent_80dbm_far",
                                                                     "CASE_TYPE_LVL1",
                                                                     "CASE_TYPE_LVL2",
                                                                     "CASE_TYPE_LVL3",
                                                                     "CASE_TYPE_LVL4",
                                                                     "CASE_TYPE_LVL5",
                                                                     "CASE_STATUS"). \
            withColumn("less_twenty_devices", func.when(col("device_score") <= 40, 1).otherwise(0)). \
            withColumn("total_2G_devices", func.when(col("interface") == "WiFi 2.4G", 1).otherwise(0))
        # Intermittancy Level

        total_count_of_devices = filter_data_accessibility.groupBy("gateway_macaddress", "polling_date"). \
            agg({"associated_device_macaddress": "count"}). \
            withColumnRenamed("count(associated_device_macaddress)", "total_count")

        less_twenty_devices = filter_data_accessibility. \
            groupBy("gateway_macaddress", "polling_date"). \
            agg({"less_twenty_devices": "sum"}). \
            withColumnRenamed("sum(less_twenty_devices)", "less_than_twenty")

        total_2G_devices = filter_data_accessibility. \
            groupBy("gateway_macaddress", "polling_date"). \
            agg({"total_2G_devices": "sum"}). \
            withColumnRenamed("sum(total_2G_devices)", "total_2G_devices")

        intermittancy_level = self.obj.join_three_frames(less_twenty_devices, total_2G_devices, total_count_of_devices, "outer",
                                                ["gateway_macaddress", "polling_date"])

        intermittancy_level = intermittancy_level.withColumn("intermittancy_perc", func.round(
            (((col("less_than_twenty") / col("total_count")) + (col("total_2G_devices") / col("total_count"))) / 2),
            2)). \
            select("gateway_macaddress", "polling_date", "intermittancy_perc")

        filter_data_accessibility_intermittancy = self.obj.join_two_frames(filter_data_accessibility, intermittancy_level,
                                                                  "left",
                                                                  ["gateway_macaddress", "polling_date"])

        output = filter_data_accessibility_intermittancy.withColumn("ticket_count",
                                                                    func.when((col("CASE_TYPE_LVL3") == "No Ticket") |
                                                                              (col("CASE_TYPE_LVL4") == "No Ticket"),
                                                                              lit(0)).otherwise(lit(1)))
        # Re-scaling the accessibility_perc_full_day

        output = output.withColumn("scaled_accessibility_perc_full_day", (col("accessibility_perc_full_day") / 100))
        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_intermittancy_percentage")

        output.select("gateway_macaddress",
                      "polling_date",
                      "associated_device_macaddress",
                      "scaled_accessibility_perc_full_day",
                      "percent_outlier_far",
                      "percent_outlier_near",
                      "percent_80dbm_near",
                      "percent_80dbm_far",
                      "intermittancy_perc",
                      "ticket_count"). \
            drop_duplicates(subset=["gateway_macaddress",
                                    "polling_date",
                                    "associated_device_macaddress",
                                    "scaled_accessibility_perc_full_day",
                                    "percent_outlier_far",
                                    "percent_outlier_near",
                                    "percent_80dbm_near",
                                    "percent_80dbm_far",
                                    "intermittancy_perc",
                                    "ticket_count"]). \
            write.saveAsTable("lwc.tmp_single_modem_intermittancy_percentage")

        # # Median percent_80dbm_far
        #
        # magic_percentile = func.expr("percentile_approx(percent_80dbm_far, 0.5)")
        #
        # f2 = filter_data.groupBy("gateway_macaddress", "polling_date").agg(magic_percentile.alias('median_percent_80dbm_far'))



        return True
