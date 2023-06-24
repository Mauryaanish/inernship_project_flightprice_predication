import sys
from dataclasses import dataclass

import numpy as np 
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder ,StandardScaler

from src.exception import CustomException
from src.logger import logging
import os
from src.utils import save_object

@dataclass
class DataTransformationConfig:
    preprocessor_obj_file_path=os.path.join('artifacts','preprocessor.pkl')

class DataTransformation:
    def __init__(self):
        self.data_transformation_config=DataTransformationConfig()

    def get_data_transformation_object(self):
        try:
            logging.info('Data Transformation initiated')


            # Define which columns should be ordinal-encoded and which should be scaled

            categorical_cols = ['Airline', 'Source', 'Destination' , ]
            numerical_cols = ['Total_Stops' , 'Journey_Day', 'Journey_Month', 'Dep_hour', 'Dep_min','Arrival_hour', 'Arrival_min', 'Duration_hour', 'Duration_min']

            logging.info('Pipeline Initiated')


            # numerical pipeline
            num_pipeline = Pipeline(
                            steps =[
                            ("imputer" , SimpleImputer( strategy='median', fill_value=None)),
                            ("scaler" , StandardScaler())
                            ]
                            )
            #categorical pipeline
            cat_pipeline = Pipeline(
                            steps=[
                            ("imputer" ,  SimpleImputer(strategy = "most_frequent")),
                            ("one_hot_encoder" , OneHotEncoder(sparse = False , handle_unknown='ignore')),
                            ("scaler" , StandardScaler())

                            ]
                            )

            preprocessor = ColumnTransformer(
            [
                ("num_pipeline" , num_pipeline ,numerical_cols),
                ("cat_pipeline" , cat_pipeline ,categorical_cols)
                
            ])

            return preprocessor
            logging.info("pipeline process complete")

        except  Exception as e:
            logging.info('Exception occured in the initiate_datatransformation')
            raise CustomException(e,sys)
        

    def initaite_data_transformation(self,train_path,test_path):
            try:
                # Reading train and test data
                train_df = pd.read_csv(train_path)
                test_df = pd.read_csv(test_path)
                
                total_stops_map = {'non-stop' : 0 , '1 stop' : 1 , '2 stops' : 2 , '3 stops' : 3 , '4 stops' : 4}
                train_df['Total_Stops'] =train_df['Total_Stops'].replace(total_stops_map)

                test_df['Total_Stops'] =test_df['Total_Stops'].replace(total_stops_map)
                
                train_df['Date_of_Journey'] = pd.to_datetime(train_df['Date_of_Journey'])

                test_df['Date_of_Journey'] = pd.to_datetime(test_df['Date_of_Journey'])
                # Extracting 'Date_of_Journey' Column into 'Journey_Month' & 'Journey_Date' Column: and journey year is not required
                train_df['Journey_Day'] = train_df['Date_of_Journey'].dt.day
                test_df['Journey_Day'] = test_df['Date_of_Journey'].dt.day

                train_df['Journey_Day'] = train_df['Journey_Day'].fillna(0)
                test_df['Journey_Day'] = test_df['Journey_Day'].fillna(0)

                train_df['Journey_Day'] = train_df['Journey_Day'].astype('int')
                test_df['Journey_Day'] = test_df['Journey_Day'].astype('int')

                train_df['Journey_Month'] = train_df['Date_of_Journey'].dt.month
                test_df['Journey_Month'] = test_df['Date_of_Journey'].dt.month

                train_df['Journey_Month'] = train_df['Journey_Month'].fillna(0)
                test_df['Journey_Month'] = test_df['Journey_Month'].fillna(0)

                train_df['Journey_Month'] = train_df['Journey_Month'].astype('int')
                test_df['Journey_Month'] = test_df['Journey_Month'].astype('int')

                train_df['Dep_hour'] = train_df['Date_of_Journey'].dt.hour
                test_df['Dep_hour'] = test_df['Date_of_Journey'].dt.hour

                train_df['Dep_hour'] = train_df['Dep_hour'].fillna(0)
                test_df['Dep_hour'] = test_df['Dep_hour'].fillna(0)

                train_df['Dep_hour'] = train_df['Dep_hour'].astype('int')
                test_df['Dep_hour'] = test_df['Dep_hour'].astype('int')


                


                train_df['Dep_min'] = train_df['Date_of_Journey'].dt.minute
                test_df['Dep_min'] = test_df['Date_of_Journey'].dt.minute

                train_df['Dep_min'] = train_df['Dep_min'].fillna(0)
                test_df['Dep_min'] = test_df['Dep_min'].fillna(0)

                train_df['Dep_min'] = train_df['Dep_min'].astype('int')
                test_df['Dep_min'] = test_df['Dep_min'].astype('int')


                train_df["Arrival_Time"]=pd.to_datetime(train_df['Arrival_Time'])
                test_df["Arrival_Time"]=pd.to_datetime(test_df['Arrival_Time'])

                train_df['Arrival_Day'] = train_df['Arrival_Time'].dt.day
                test_df['Arrival_Day'] = test_df['Arrival_Time'].dt.day

                train_df['Arrival_Day'] = train_df['Arrival_Day'].fillna(0)
                test_df['Arrival_Day'] = test_df['Arrival_Day'].fillna(0)

                train_df['Arrival_Day'] = train_df['Arrival_Day'].astype('int')
                test_df['Arrival_Day'] = test_df['Arrival_Day'].astype('int')

                train_df['Arrival_Month'] = train_df['Arrival_Time'].dt.month
                test_df['Arrival_Month'] = test_df['Arrival_Time'].dt.month

                train_df['Arrival_Month'] = train_df['Arrival_Month'].fillna(0)
                test_df['Arrival_Month'] = test_df['Arrival_Month'].fillna(0)

                train_df['Arrival_Month'] = train_df['Arrival_Month'].astype('int')
                test_df['Arrival_Month'] = test_df['Arrival_Month'].astype('int')


                train_df['Arrival_hour'] = train_df['Arrival_Time'].dt.hour
                test_df['Arrival_hour'] = test_df['Arrival_Time'].dt.hour

                train_df['Arrival_hour'] = train_df['Arrival_hour'].fillna(0)
                test_df['Arrival_hour'] = test_df['Arrival_hour'].fillna(0)

                train_df['Arrival_hour'] = train_df['Arrival_hour'].astype('int')
                test_df['Arrival_hour'] = test_df['Arrival_hour'].astype('int')

                train_df['Arrival_min'] = train_df['Arrival_Time'].dt.minute
                test_df['Arrival_hour'] = test_df['Arrival_Time'].dt.minute

                train_df['Arrival_min'] = train_df['Arrival_min'].fillna(0)
                test_df['Arrival_hour'] = test_df['Arrival_min'].fillna(0)

                train_df['Arrival_min'] = train_df['Arrival_min'].astype('int')
                test_df['Arrival_hour'] = test_df['Arrival_min'].astype('int')

                train_df['Duration_hour'] =  (train_df['Arrival_hour']) - (train_df['Dep_hour']) 
                train_df['Duration_hour'] = train_df['Duration_hour'].fillna(0)
                train_df['Duration_hour'] = train_df['Duration_hour'].astype('int')

                test_df['Duration_hour'] =   (test_df['Arrival_hour']) - (test_df['Dep_hour'])
                test_df['Duration_hour'] = test_df['Duration_hour'].fillna(0)
                test_df['Duration_hour'] = test_df['Duration_hour'].astype('int')

                train_df['Duration_min'] = (train_df['Arrival_min']) - (train_df['Dep_min'])
                train_df['Duration_min'] = train_df['Duration_min'].fillna(0)
                train_df['Duration_min'] = train_df['Duration_min'].astype('int')

                test_df['Duration_min'] = (test_df['Arrival_min']) - (test_df['Dep_min']) 
                test_df['Duration_min'] = test_df['Duration_min'].fillna(0)
                test_df['Duration_min'] = test_df['Duration_min'].astype('int')

 

                train_df.drop(['Date_of_Journey' , 'Route' , 'Dep_Time' ,'Arrival_Time' , 'Duration' , 'Additional_Info' ] , axis = 1 , inplace = True)
                test_df.drop(['Date_of_Journey' , 'Route' , 'Dep_Time' ,'Arrival_Time' , 'Duration' , 'Additional_Info' ] , axis = 1 , inplace = True)
                


                logging.info('Read train and test data completed')
                logging.info(f'Train Dataframe Head : \n{train_df.head().to_string()}')
                logging.info(f'Test Dataframe Head  : \n{test_df.head().to_string()}')

                logging.info('Obtaining preprocessing object')

                preprocessing_obj = self.get_data_transformation_object()

                target_column_name = 'Price'
                drop_columns = [target_column_name]

                input_feature_train_df = train_df.drop(columns=drop_columns,axis=1)
                target_feature_train_df=train_df[target_column_name]

                input_feature_test_df=test_df.drop(columns=drop_columns,axis=1)
                target_feature_test_df=test_df[target_column_name]
                
                ## Trnasformating using preprocessor obj
                input_feature_train_arr=preprocessing_obj.fit_transform(input_feature_train_df)
                input_feature_test_arr=preprocessing_obj.transform(input_feature_test_df)

                logging.info("Applying preprocessing object on training and testing datasets.")
                

                train_arr = np.c_[input_feature_train_arr, np.array(target_feature_train_df)]
                test_arr = np.c_[input_feature_test_arr, np.array(target_feature_test_df)]

                save_object(

                    file_path=self.data_transformation_config.preprocessor_obj_file_path,
                    obj=preprocessing_obj

                )
                logging.info('Preprocessor pickle file saved')

                return (
                    train_arr,
                    test_arr,
                    self.data_transformation_config.preprocessor_obj_file_path,
                )
                
            except Exception as e:
                logging.info("Exception occured in the initiate_datatransformation")

                raise CustomException(e,sys)