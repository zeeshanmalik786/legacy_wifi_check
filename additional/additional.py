from preprocessor.preprocess import preprocessor
from config.config import config
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col
from pyspark.sql.functions import lit
from pyspark.sql.functions import countDistinct


class addition_telemarkers:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def additions(self):

        data = self.obj.get_data("business_users_test.tmp_single_modems_v1", ["gateway_macaddress",
                                                                            "associated_device_macaddress",
                                                                            "interface",
                                                                            "unified_ssid",
                                                                            "percent_outlier_near",
                                                                            "percent_outlier_far",
                                                                            "percent_80dbm_near",
                                                                            "percent_80dbm_far",
                                                                            "signalstrength",
                                                                            "device_score",
                                                                            "norm_signalstrength",
                                                                            "Rating",
                                                                            "polling_date",
                                                                            "polling_span",
                                                                            "devicename",
                                                                            "hardwareversion",
                                                                            "missing_data",
                                                                            "missing_data_device",
                                                                            "predicted_lowerbound",
                                                                            "predicted_upperbound",
                                                                            "outlier",
                                                                            "less_than_80dbm"])

        df_modem_accessibility = self.obj.get_data("pma.modem_accessibility_daily", ["mac",
                                                                                     "full_account_number",
                                                                                     "event_date"]).\
            withColumnRenamed("mac", "gateway_macaddress").\
            withColumnRenamed("event_date", "polling_date").\
            filter(col("modem_manufacturer") == "HITRON")

        # CBC Tickets

        df_icm_cases = self.obj.get_data("business_users_test.etl_icm_case", ["CASE_ID",
                                                                              "CASE_NUMBER",
                                                                              "CASE_CREATED_DATE",
                                                                              "CASE_FA",
                                                                              "CASE_TYPE_LVL1",
                                                                              "CASE_TYPE_LVL2",
                                                                              "CASE_TYPE_LVL3",
                                                                              "CASE_TYPE_LVL4",
                                                                              "CASE_TYPE_LVL5",
                                                                              "CASE_STATUS"]). \
            withColumn("polling_date", func.to_date(func.col("CASE_CREATED_DATE")))

        df_icm_cases = df_icm_cases.\
            withColumnRenamed("CASE_FA", "full_account_number")

        df_modem_icm = self.obj.join_two_frames(df_modem_accessibility, df_icm_cases, "inner", ["full_account_number",
                                                                                                "polling_date"])

        # df_modem_icm.groupBy("polling_date").agg(countDistinct("full_account_number")).show()

        combined_data_cbc = self.obj.join_two_frames(data, df_modem_icm,"left",["gateway_macaddress", "polling_date"])

        combined_data_cbc = combined_data_cbc.\
            withColumn("CASE_TYPE_LVL1",func.when(col("CASE_TYPE_LVL1").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL1"))). \
            withColumn("CASE_TYPE_LVL2",func.when(col("CASE_TYPE_LVL2").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL2"))). \
            withColumn("CASE_TYPE_LVL3",func.when(col("CASE_TYPE_LVL3").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL3"))). \
            withColumn("CASE_TYPE_LVL4",func.when(col("CASE_TYPE_LVL4").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL4"))). \
            withColumn("CASE_TYPE_LVL5",func.when(col("CASE_TYPE_LVL5").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL5"))). \
            withColumn("CASE_STATUS", func.when(col("CASE_STATUS").isNull(), "No Status").otherwise(col("CASE_STATUS")))


        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_additional")

        combined_data_cbc.write.saveAsTable("business_users_test.tmp_single_modem_additional")

        return True










































