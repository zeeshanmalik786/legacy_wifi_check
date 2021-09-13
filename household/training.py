import lightgbm as lgb
from pyspark.sql import functions as func
from pyspark.sql.functions import rank, col, concat
from pyspark.sql.functions import countDistinct
import pyspark.sql.functions as f
from pyspark.sql.functions import lit
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pickle
import os
from sklearn.externals import joblib
from sklearn.metrics import confusion_matrix
from preprocessor.preprocess import preprocessor
from config.config import config

class training:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def training(self):
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

        data = self.obj.get_data("business_users_test.tmp_single_modem_training_set", ["gateway_macaddress",
                                                                              "intermittancy_perc",
                                                                              "scaled_accessibility_perc_full_day",
                                                                              "ticket_count",
                                                                              "percent_outlier_far_between_0_to_79",
                                                                              "percent_outlier_far_between_80_to_100",
                                                                              "percent_outlier_near_between_0_to_79",
                                                                              "percent_outlier_near_between_80_to_100",
                                                                              "percent_80dbm_near_between_0_to_79",
                                                                              "percent_80dbm_near_between_80_to_100",
                                                                              "percent_80dbm_far_between_0_to_79",
                                                                              "percent_80dbm_far_between_80_to_100",
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
                  "percent_outlier_far_between_0_to_79",
                  "percent_outlier_far_between_80_to_100",
                  "percent_outlier_near_between_0_to_79",
                  "percent_outlier_near_between_80_to_100",
                  "percent_80dbm_near_between_0_to_79",
                  "percent_80dbm_near_between_80_to_100",
                  "percent_80dbm_far_between_0_to_79",
                  "percent_80dbm_far_between_80_to_100"
                  ]]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=7)
        train_data = lgb.Dataset(X_train, label=y_train, free_raw_data=False)
        gbm = lgb.train(
            params,
            train_data,
            num_boost_round=30)
        print('begin to predict data')
        y_pred = gbm.predict(X_test)
        predictions = [round(value) for value in y_pred]

        accuracy = accuracy_score(y_test, predictions)
        print("Accuracy: %.2f%%" % (accuracy * 100.0))
        cm = confusion_matrix(y_test, predictions)
        print(cm)

