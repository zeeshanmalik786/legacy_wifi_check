from recommendation.preprocess import preprocessor
from recommendation.config import config
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col, concat
from pyspark.sql.functions import countDistinct


class recommendation:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def recommendation(self, run_date):

        data = self.obj.get_data("lwc.tmp_single_modem_additional",["gateway_macaddress",
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
                                                                            "CASE_STATUS"])
        # Need to change for Production Purposes
        # count_span = data.\
        #     groupBy("gateway_macaddress").agg(countDistinct("polling_date")).\
        #     withColumnRenamed("count(DISTINCT polling_date)", "all_polls")

        count_span = data. \
            groupBy("gateway_macaddress").agg(countDistinct("polling_date")). \
            withColumnRenamed("count(polling_date)", "all_polls")

        # Need to change for Production Purposes
        # overnight = data.filter(col("polling_span") == "Overnight").\
        #     groupBy("gateway_macaddress").agg(countDistinct("polling_date")).\
        #     withColumnRenamed("count(DISTINCT polling_date)", "overnight_polls")

        overnight = data.filter(col("polling_span") == "Overnight"). \
            groupBy("gateway_macaddress").agg(countDistinct("polling_date")). \
            withColumnRenamed("count(polling_date)", "overnight_polls")

        # Need to change for Production Purposes
        # daylight =  data.filter(col("polling_span") == "Daylight").\
        #     groupBy("gateway_macaddress").agg(countDistinct("polling_date")).\
        #     withColumnRenamed("count(DISTINCT polling_date)", "daylight_polls")

        daylight = data.filter(col("polling_span") == "Daylight"). \
            groupBy("gateway_macaddress").agg(countDistinct("polling_date")). \
            withColumnRenamed("count(polling_date)", "daylight_polls")

        join_overnight = self.obj.join_two_frames(data, overnight, "left",
                                                  "gateway_macaddress")

        join_overnight_daylight = self.obj.join_two_frames(join_overnight, daylight,"left",
                                                           "gateway_macaddress")

        join_overnight_daylight_total = self.obj.join_two_frames(join_overnight_daylight,count_span,"left",
                                                                 "gateway_macaddress")

        join_overnight_daylight_total. \
            withColumn("perc_overnight", func.round((100 * (col("overnight_polls")/col("all_polls"))), 2)).\
            withColumn("perc_daylight", func.round((100 * (col("daylight_polls")/col("all_polls"))), 2))

        # PoDs Recommendation

        filter_devices = join_overnight_daylight_total.\
            filter(col("polling_date") == run_date)

        struggling_devices = filter_devices.\
            filter(col("device_score") < 40).\
            groupBy("gateway_macaddress").\
            agg({"gateway_macaddress": "count"}).\
            withColumnRenamed("count(gateway_macaddress)", "intermittent_device_count")

        total_devices = filter_devices.\
            groupBy("gateway_macaddress").\
            agg({"gateway_macaddress": "count"}). \
            withColumnRenamed("count(gateway_macaddress)", "total_device_count")

        pods_reco_1 = self.obj.join_two_frames(filter_devices, struggling_devices,"left", "gateway_macaddress")

        pods_reco_1 = pods_reco_1.withColumn("intermittent_device_count",
                                                           func.when(col("intermittent_device_count").isNotNull(),
                                                                     col("intermittent_device_count")).\
                                                           otherwise(0))

        pods_reco_2 = self.obj.join_two_frames(pods_reco_1,total_devices, "left", "gateway_macaddress").\
            withColumn("perc_intermittent", (100 * (col("intermittent_device_count")/col("total_device_count"))))

        pods_recommendation = pods_reco_2.withColumn("pods_reco",func.when(col("perc_intermittent") > 80, "Yes").\
                                                     otherwise("No"))

        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_recommendation")


        pods_recommendation.write.saveAsTable("lwc.tmp_single_modem_recommendation")

        return True