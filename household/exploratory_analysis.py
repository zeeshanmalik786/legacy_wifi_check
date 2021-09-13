from preprocessor.preprocess import preprocessor
from config.config import config
import pandas as pd
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt


class dimensionality:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def visualization(self):

        data = self.obj.get_data("business_users_test.tmp_single_modem_training_set",["intermittancy_perc",
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

        X_m = data[["intermittancy_perc",
                    "scaled_accessibility_perc_full_day",
                    "ticket_count",
                    "percent_outlier_far_between_0_to_79",
                    "percent_outlier_far_between_80_to_100",
                    "percent_outlier_near_between_0_to_79",
                    "percent_outlier_near_between_80_to_100",
                    "percent_80dbm_near_between_0_to_79",
                    "percent_80dbm_near_between_80_to_100",
                    "percent_80dbm_far_between_0_to_79",
                    "percent_80dbm_far_between_80_to_100"]]

        y = data.label

        pca = PCA(n_components=2)
        principalComponents = pca.fit_transform(X_m)
        principalDf = pd.DataFrame(data=principalComponents
                                   , columns=['principal component 1', 'principal component 2'])

        finalDf = pd.concat([principalDf, y], axis=1)

        fig = plt.figure(figsize=(8, 8))
        ax = fig.add_subplot(1, 1, 1)
        ax.set_xlabel('Principal Component 1', fontsize=15)
        ax.set_ylabel('Principal Component 2', fontsize=15)
        ax.set_title('2 component PCA', fontsize=20)
        targets = [0,1]
        colors = ['r', 'b']
        for target, color in zip(targets, colors):
            indicesToKeep = finalDf['label'] == target
            ax.scatter(finalDf.loc[indicesToKeep, 'principal component 1']
                       , finalDf.loc[indicesToKeep, 'principal component 2']
                       , c=color
                       , s=50)
        ax.legend(targets)
        ax.grid()
        plt.show()







