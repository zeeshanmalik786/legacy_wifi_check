import xgboost as xgb
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from household.preprocess import preprocessor
from household.config import config

class train:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def train(self):
        params = {
            'task': 'train',
            'boosting_type': 'gbdt',
            'objective': 'binary',
            'metric': {'l2', 'auc', 'binary'},
            'is_training_metric': True,
            'metric_freq': 5,
            'num_leaves': 31,
            'learning_rate': 0.5,
            'feature_fraction': 0.9,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': 1,
            'device': 'cpu',
        }
        self.spark.sql("REFRESH TABLE lwc.tmp_single_modem_training_set")

        data = self.obj.get_data("lwc.tmp_single_modem_training_set", ["gateway_macaddress",
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

        return True
