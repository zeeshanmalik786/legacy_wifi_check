from additional.preprocess import preprocessor
from additional.config import config
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col, upper
from pyspark.sql.functions import lit, when
from pyspark.sql.functions import regexp_replace


class addition_telemarkers:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def additions(self, run_date):

        data = self.obj.get_data("lwc.tmp_single_modems_v1", ["gateway_macaddress",
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

        #Need to enable when pushing to production
        # df_modem_accessibility = self.obj.get_data("default.modem_accessibility_daily", ["mac",
        #                                                                              "full_account_number",
        #                                                                              "event_date"]).\
        #     withColumnRenamed("mac", "gateway_macaddress").\
        #     withColumnRenamed("event_date", "polling_date").\
        #     filter(col("modem_manufacturer") == "HITRON")


        #temperory for validation

        df_modem_accessibility = self.obj.get_data("hem.modem_accessibility_daily", ["mac",
                                                                                         "full_account_number",
                                                                                         "event_date"]).\
            withColumnRenamed("event_date", "polling_date").\
            withColumnRenamed("mac", "gateway_macaddress")

        # CBC Tickets


        df_icm_cases_one = self.obj.get_data("ela_crm.table_case",  ["CASE2FIN_ACCNT",
                                                                     "id_number",
                                                                     "creation_time",
                                                                     "case_type_lvl1",
                                                                     "case_type_lvl2",
                                                                     "case_type_lvl3",
                                                                     "x_case_type_lvl4",
                                                                     "x_case_type_lvl5"]).\
            withColumnRenamed("CASE2FIN_ACCNT", "OBJID").\
            withColumnRenamed("id_number", "CASE_NUMBER").\
            withColumnRenamed("creation_time", "CASE_CREATED_DATE").\
            withColumnRenamed("case_type_lvl1","CASE_TYPE_LVL1").\
            withColumnRenamed("case_type_lvl2", "CASE_TYPE_LVL2").\
            withColumnRenamed("case_type_lvl3", "CASE_TYPE_LVL3").\
            withColumnRenamed("x_case_type_lvl4", "CASE_TYPE_LVL4").\
            withColumnRenamed("x_case_type_lvl5", "CASE_TYPE_LVL5")

        df_icm_cases_two = self.obj.get_data("ela_crm.table_fin_accnt", ["S_FA_ID",
                                                                         "OBJID"])

        df_icm_cases = self.obj.join_two_frames(df_icm_cases_one,df_icm_cases_two,"left",["OBJID"])


        df_icm_cases = df_icm_cases.select("S_FA_ID",
                   "CASE_NUMBER",
                   "CASE_CREATED_DATE",
                   "CASE_TYPE_LVL1",
                   "CASE_TYPE_LVL2",
                   "CASE_TYPE_LVL3",
                   "CASE_TYPE_LVL4",
                   "CASE_TYPE_LVL5").\
            withColumnRenamed("CASE_CREATED_DATE", "polling_date").\
            withColumnRenamed("S_FA_ID", "full_account_number")

        df_modem_icm = self.obj.join_two_frames(df_modem_accessibility, df_icm_cases, "inner", ["full_account_number",
                                                                                                "polling_date"]).\
            withColumn("gateway_macaddress", upper(regexp_replace("gateway_macaddress","-","")))

        combined_data_cbc = self.obj.join_two_frames(data, df_modem_icm,"left",["gateway_macaddress", "polling_date"])

        combined_data_cbc = combined_data_cbc.\
            withColumn("CASE_TYPE_LVL1",func.when(col("CASE_TYPE_LVL1").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL1"))). \
            withColumn("CASE_TYPE_LVL2",func.when(col("CASE_TYPE_LVL2").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL2"))). \
            withColumn("CASE_TYPE_LVL3",func.when(col("CASE_TYPE_LVL3").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL3"))). \
            withColumn("CASE_TYPE_LVL4",func.when(col("CASE_TYPE_LVL4").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL4"))). \
            withColumn("CASE_TYPE_LVL5",func.when(col("CASE_TYPE_LVL5").isNull(), "No Ticket").otherwise(col("CASE_TYPE_LVL5"))).\
            withColumn("CASE_STATUS", lit("Passed")).\
            withColumn("CASE_ID", lit(1))


        self.spark.sql("DROP TABLE IF EXISTS lwc.tmp_single_modem_additional")

        combined_data_cbc.write.saveAsTable("lwc.tmp_single_modem_additional")

        return True










































