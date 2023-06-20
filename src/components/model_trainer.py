import sys
import os
import numpy as np
import pandas as pd
from src.exception import CustomException
from src.logger import logging
# import sklearn model

from sklearn.ensemble import RandomForestRegressor
# import metrics

from sklearn.metrics import r2_score

from src.utils import save_object
from dataclasses import dataclass


@dataclass 
class ModelTrainerConfig:
    trained_model_file_path = os.path.join('artifacts','model.pkl')


class ModelTrainer:
    def __init__(self):
        self.model_trainer_config = ModelTrainerConfig()

    def initate_model_training(self,train_array,test_array):
        try:
            logging.info('Splitting Dependent and Independent variables from train and test data')
            X_train, y_train, X_test, y_test = (
                train_array[:,:-1],
                train_array[:,-1],
                test_array[:,:-1],
                test_array[:,-1]
            )

            # create model object
            model = RandomForestRegressor()
            # fit a data in model 

            model.fit(X_train,y_train)

            # predit model

            y_pred = model.predict(X_test)

            # get model score

            score = r2_score(y_pred,y_test)

            print(f'Model Name:- {model} | Model Score:- {score}')

            logging.info(f'Model Name:- {model} | Model Score:- {score}')

            save_object(
                    file_path=self.model_trainer_config.trained_model_file_path,
                    obj=model
                )

        except Exception as e:
                logging.info('Exception occured at Model Training')
                raise CustomException(e,sys)