import os
import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from dataclasses import dataclass
from src.components.data_transformation import DataTransformation

@dataclass
class DataIngestionconfig:
    train_data_path:str=os.path.join('artifacts' , 'train.csv')
    test_data_path:str = os.path.join('artifacts' , 'test.csv')
    raw_data_path:str = os.path.join('artifacts' , 'raw.csv')

## create a class for Data Ingestion

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionconfig()

    def initiate_data_ingestion(self):
        logging.info("Data Ingestion Method Starts")
        try:
            df = pd.read_excel(os.path.join('notebooks/data' , 'flight_price.xlsx'))
            logging.info('Dateset read as pandas Dataframe')

            logging.info('Cleaning Parth Start')

            # Stops maping non-stop : 0 , 1-stop : 1 , 2-stop : 2 , 3-stop : 3 , 4-stop : 4
            total_stops_map = {'non-stop' : 0 , '1 stop' : 1 , '2 stops' : 2 , '3 stops' : 3 , '4 stops' : 4}
            df['Total_Stops'] = df['Total_Stops'].replace(total_stops_map)
            
            df['Date_of_Journey'] = pd.to_datetime(df['Date_of_Journey'])
            # Extracting 'Date_of_Journey' Column into 'Journey_Month' & 'Journey_Date' Column: and journey year is not required
            df['Journey_Day'] = df['Date_of_Journey'].dt.day
            df['Journey_Month'] = df['Date_of_Journey'].dt.month

            # Extracting Dep_Time columns into Dep_hours and Dep_min
            df['Dep_hour'] = df['Dep_Time'].str.split(':').str[0]
            df['Dep_min'] = df['Dep_Time'].str.split(':').str[1]

            # change Date type object to int

            df['Dep_hour'] = df['Dep_hour'].astype('int')
            df['Dep_min'] = df['Dep_min'].astype('int')

            df["Arrival_Time"]=df["Arrival_Time"].str.split(" ").str[0]

            # Extracting Arrival_Time columns into Arrival_hour and Arrival_min 
            df["Arrival_hour"]=df["Arrival_Time"].str.split(":").str[0]
            df["Arrival_min"] = df["Arrival_Time"].str.split(':').str[1]

            # change date type object to int

            df["Arrival_hour"] = df["Arrival_hour"].astype('int')
            df["Arrival_min"] = df["Arrival_min"].astype('int')

            # Extracting Duration  columns into Duration_hour and Duration_min 
            df['Duration_hour'] = df['Duration'].str.split().str[0]
            df['Duration_min'] = df['Duration'].str.split().str[1]

            # remove 'm' in data
            df['Duration_min']=df['Duration_min'].str.replace('m' , " ") 

            df['Duration_min']=df['Duration_min'].replace('nan' , np.nan)

            # remove 'h' in data
            df['Duration_hour']=df['Duration_hour'].str.replace('h' , ' ')

            df[df['Duration_hour'] == '5m']

            df.drop(6474 , axis = 0 , inplace = True)
            # Change data type object to int
            df['Duration_hour'] = df['Duration_hour'].fillna(0)
            df['Duration_hour'] = df['Duration_hour'].astype('int')

            # some nan value in Duration_min columns so replace nan value with 0

            df['Duration_min'] = df['Duration_min'].fillna(0)
            # change Data type oject to int

            df['Duration_min'] = df['Duration_min'].astype('int')

            # drop columns 

            #df.drop(['Date_of_Journey' , 'Route' , 'Dep_Time' ,'Arrival_Time' , 'Duration' , 'Additional_Info' ] , axis = 1 , inplace = True)

            logging.info('Data Cealing Part is Done')

            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path) , exist_ok=True)

            df.to_csv(self.ingestion_config.raw_data_path , index = False)

            logging.info('Train Test Split')
            train_set , test_set = train_test_split(df , test_size=0.3 , random_state= 42)
            train_set.to_csv(self.ingestion_config.train_data_path , index = False , header = True)
            test_set.to_csv(self.ingestion_config.test_data_path , index = False , header = True)

            logging.info('Ingetion of data is completed')

            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path )



        except Exception as e:

            logging.info("Exception occured at data Ingestion stage")
            raise CustomException(e , sys)