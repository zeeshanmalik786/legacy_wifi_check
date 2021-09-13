import xgboost as xgb
from sklearn.metrics import confusion_matrix,accuracy_score, roc_curve, auc
import datetime
from datetime import datetime
from datetime import timedelta
from pytz import timezone
from sklearn.preprocessing import LabelEncoder
from household.config import config
from household.preprocess import preprocessor

class train_test:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def train_test(self, run_date):
        fmt = "%Y-%m-%d"
        tz = timezone('EST')
        current_date = datetime.now(tz).strftime(fmt)
        run_date = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=-1)
        print(run_date)

        self.spark.sql("REFRESH TABLE test.tmp_single_modem_training_set")

        data = self.obj.get_data("test.tmp_single_modem_training_set", ["gateway_macaddress",
                                                                              "intermittancy_perc",
                                                                              "scaled_accessibility_perc_full_day",
                                                                              "ticket_count",
                                                                              "percent_outlier_far_between_0_to_20",
                                                                              "percent_outlier_far_between_20_to_40",
                                                                              "percent_outlier_far_between_40_to_60",
                                                                              "percent_outlier_far_between_60_to_80",
                                                                              "percent_outlier_far_between_80_to_100",
                                                                              "percent_outlier_near_between_0_to_20",
                                                                              "percent_outlier_near_between_20_to_40",
                                                                              "percent_outlier_near_between_40_to_60",
                                                                              "percent_outlier_near_between_60_to_80",
                                                                              "percent_outlier_near_between_80_to_100",
                                                                              "percent_80dbm_near_between_0_to_20",
                                                                              "percent_80dbm_near_between_20_to_40",
                                                                              "percent_80dbm_near_between_40_to_60",
                                                                              "percent_80dbm_near_between_60_to_80",
                                                                              "percent_80dbm_near_between_80_to_100",
                                                                              "percent_80dbm_far_between_0_to_20",
                                                                              "percent_80dbm_far_between_20_to_40",
                                                                              "percent_80dbm_far_between_40_to_60",
                                                                              "percent_80dbm_far_between_60_to_80",
                                                                              "percent_80dbm_far_between_80_to_100",
                                                                              "ticket_moving_average_between_80_to_100",
                                                                              "ticket_moving_average_between_60_to_80",
                                                                              "ticket_moving_average_between_40_to_60",
                                                                              "ticket_moving_average_between_20_to_40",
                                                                              "ticket_moving_average_between_0_to_20",
                                                                              "label"]).toPandas()

        encoder = LabelEncoder()
        encoder.fit(data["gateway_macaddress"].values)
        encoded_GW = encoder.transform(data["gateway_macaddress"])
        data["encoded_GW"] = encoded_GW
        y = data[["label"]]
        X = data[["encoded_GW",
                  "intermittancy_perc",
                  "scaled_accessibility_perc_full_day",
                  "ticket_count",
                  "percent_outlier_far_between_0_to_20",
                  "percent_outlier_far_between_20_to_40",
                  "percent_outlier_far_between_40_to_60",
                  "percent_outlier_far_between_60_to_80",
                  "percent_outlier_far_between_80_to_100",
                  "percent_outlier_near_between_0_to_20",
                  "percent_outlier_near_between_20_to_40",
                  "percent_outlier_near_between_40_to_60",
                  "percent_outlier_near_between_60_to_80",
                  "percent_outlier_near_between_80_to_100",
                  "percent_80dbm_near_between_0_to_20",
                  "percent_80dbm_near_between_20_to_40",
                  "percent_80dbm_near_between_40_to_60",
                  "percent_80dbm_near_between_60_to_80",
                  "percent_80dbm_near_between_80_to_100",
                  "percent_80dbm_far_between_0_to_20",
                  "percent_80dbm_far_between_20_to_40",
                  "percent_80dbm_far_between_40_to_60",
                  "percent_80dbm_far_between_60_to_80",
                  "percent_80dbm_far_between_80_to_100",
                  "ticket_moving_average_between_80_to_100",
                  "ticket_moving_average_between_60_to_80",
                  "ticket_moving_average_between_40_to_60",
                  "ticket_moving_average_between_20_to_40",
                  "ticket_moving_average_between_0_to_20"
                  ]]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=7)

        data_dmatrix = xgb.DMatrix(data=X, label=y)

        gbm = xgb.XGBRegressor(objective='reg:linear', colsample_bytree=0.3, learning_rate=0.1,
                               max_depth=5, alpha=10, n_estimators=10)

        gbm.fit(X_train, y_train)

        y_pred = gbm.predict(X_test)

        regression = y_pred
        predictions = [round(value) for value in y_pred]

        accuracy = accuracy_score(y_test, predictions)
        print("Accuracy: %.2f%%" % (accuracy * 100.0))
        cm = confusion_matrix(y_test, predictions)

        self.spark.sql("REFRESH TABLE test.tmp_single_modem_prediction_moving_average")

        data = self.obj.get_data("test.tmp_single_modem_prediction_moving_average", ["gateway_macaddress",
                                                                                           "polling_date",
                                                                                           "intermittancy_perc",
                                                                                           "scaled_accessibility_perc_full_day",
                                                                                           "ticket_count",
                                                                                           "percent_outlier_far_between_0_to_20",
                                                                                           "percent_outlier_far_between_20_to_40",
                                                                                           "percent_outlier_far_between_40_to_60",
                                                                                           "percent_outlier_far_between_60_to_80",
                                                                                           "percent_outlier_far_between_80_to_100",
                                                                                           "percent_outlier_near_between_0_to_20",
                                                                                           "percent_outlier_near_between_20_to_40",
                                                                                           "percent_outlier_near_between_40_to_60",
                                                                                           "percent_outlier_near_between_60_to_80",
                                                                                           "percent_outlier_near_between_80_to_100",
                                                                                           "percent_80dbm_near_between_0_to_20",
                                                                                           "percent_80dbm_near_between_20_to_40",
                                                                                           "percent_80dbm_near_between_40_to_60",
                                                                                           "percent_80dbm_near_between_60_to_80",
                                                                                           "percent_80dbm_near_between_80_to_100",
                                                                                           "percent_80dbm_far_between_0_to_20",
                                                                                           "percent_80dbm_far_between_20_to_40",
                                                                                           "percent_80dbm_far_between_40_to_60",
                                                                                           "percent_80dbm_far_between_60_to_80",
                                                                                           "percent_80dbm_far_between_80_to_100",
                                                                                           "ticket_moving_average_between_80_to_100",
                                                                                           "ticket_moving_average_between_60_to_80",
                                                                                           "ticket_moving_average_between_40_to_60",
                                                                                           "ticket_moving_average_between_20_to_40",
                                                                                           "ticket_moving_average_between_0_to_20"]).toPandas()

        encoder = LabelEncoder()
        encoder.fit(data["gateway_macaddress"].values)
        data["encoded_GW"] = encoder.transform(data["gateway_macaddress"])

        # filename='dbfs:/tmp/zeeshan1/pods_classifier.sav'
        # model = pickle.load(open(filename, 'rb'))
        X = data[["encoded_GW",
                  "intermittancy_perc",
                  "scaled_accessibility_perc_full_day",
                  "ticket_count",
                  "percent_outlier_far_between_0_to_20",
                  "percent_outlier_far_between_20_to_40",
                  "percent_outlier_far_between_40_to_60",
                  "percent_outlier_far_between_60_to_80",
                  "percent_outlier_far_between_80_to_100",
                  "percent_outlier_near_between_0_to_20",
                  "percent_outlier_near_between_20_to_40",
                  "percent_outlier_near_between_40_to_60",
                  "percent_outlier_near_between_60_to_80",
                  "percent_outlier_near_between_80_to_100",
                  "percent_80dbm_near_between_0_to_20",
                  "percent_80dbm_near_between_20_to_40",
                  "percent_80dbm_near_between_40_to_60",
                  "percent_80dbm_near_between_60_to_80",
                  "percent_80dbm_near_between_80_to_100",
                  "percent_80dbm_far_between_0_to_20",
                  "percent_80dbm_far_between_20_to_40",
                  "percent_80dbm_far_between_40_to_60",
                  "percent_80dbm_far_between_60_to_80",
                  "percent_80dbm_far_between_80_to_100",
                  "ticket_moving_average_between_80_to_100",
                  "ticket_moving_average_between_60_to_80",
                  "ticket_moving_average_between_40_to_60",
                  "ticket_moving_average_between_20_to_40",
                  "ticket_moving_average_between_0_to_20"
                  ]]

        data['household_score'] = gbm.predict(X)
        data['pods_recommendation'] = [round(value) for value in data['household_score']]
        data = data[["gateway_macaddress",
                     "intermittancy_perc",
                     "scaled_accessibility_perc_full_day",
                     "ticket_count",
                     "percent_outlier_far_between_0_to_20",
                     "percent_outlier_far_between_20_to_40",
                     "percent_outlier_far_between_40_to_60",
                     "percent_outlier_far_between_60_to_80",
                     "percent_outlier_far_between_80_to_100",
                     "percent_outlier_near_between_0_to_20",
                     "percent_outlier_near_between_20_to_40",
                     "percent_outlier_near_between_40_to_60",
                     "percent_outlier_near_between_60_to_80",
                     "percent_outlier_near_between_80_to_100",
                     "percent_80dbm_near_between_0_to_20",
                     "percent_80dbm_near_between_20_to_40",
                     "percent_80dbm_near_between_40_to_60",
                     "percent_80dbm_near_between_60_to_80",
                     "percent_80dbm_near_between_80_to_100",
                     "percent_80dbm_far_between_0_to_20",
                     "percent_80dbm_far_between_20_to_40",
                     "percent_80dbm_far_between_40_to_60",
                     "percent_80dbm_far_between_60_to_80",
                     "percent_80dbm_far_between_80_to_100",
                     "ticket_moving_average_between_80_to_100",
                     "ticket_moving_average_between_60_to_80",
                     "ticket_moving_average_between_40_to_60",
                     "ticket_moving_average_between_20_to_40",
                     "ticket_moving_average_between_0_to_20",
                     "household_score",
                     "pods_recommendation"]]

        output = self.spark.createDataFrame(data)

        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_prediction_classifier")

        output.write.saveAsTable("business_users_test.tmp_single_modem_prediction_classifier")

        self.spark.sql("REFRESH TABLE business_users_test.tmp_single_modem_prediction_classifier")

        predictions = self.obj.get_data("business_users_test.tmp_single_modem_prediction_classifier", ["gateway_macaddress",
                                                                                              "pods_recommendation"])

        self.spark.sql("REFRESH TABLE business_users_test.tmp_single_modem_recommendation")

        recommendations = self.obj.get_data("business_users_test.tmp_single_modem_recommendation", ["gateway_macaddress",
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
                                                                                           "CASE_STATUS",
                                                                                           "overnight_polls",
                                                                                           "daylight_polls",
                                                                                           "all_polls",
                                                                                           "intermittent_device_count",
                                                                                           "total_device_count",
                                                                                           "perc_intermittent",
                                                                                           "pods_reco"])

        pred_recommendation = self.obj.join_two_frames(recommendations, predictions, "inner", "gateway_macaddress")

        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_recommendation_predictions")

        pred_recommendation.write.saveAsTable("business_users_test.tmp_single_modem_recommendation_predictions")

        df = self.obj.get_data("business_users_test.tmp_single_modem_recommendation_predictions", "ALL")

        self.spark.sql("DROP TABLE IF EXISTS business_users_test.tmp_single_modem_recommendation")

        df.write.saveAsTable("business_users_test.tmp_single_modem_recommendation")






