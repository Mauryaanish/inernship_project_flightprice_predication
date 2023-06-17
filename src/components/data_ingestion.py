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
            df = pd.read_csv(os.path.join('notebooks/data' , 'flight_price_clean_data'))
            logging.info('Dateset read as pandas Dataframe')

            logging.info('Maping Total Stops in ordinal ')

            # Stops maping non-stop : 0 , 1-stop : 1 , 2-stop : 2 , 3-stop : 3 , 4-stop : 4
            total_stops_map = {'non-stop' : 0 , '1 stop' : 1 , '2 stops' : 2 , '3 stops' : 3 , '4 stops' : 4}
            df['Total_Stops'] = df['Total_Stops'].replace(total_stops_map)

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