import os
import sys
from src.exception import CustomException
from src.logger import logging
from src.utils import load_object
import pandas as pd

class PredictionPipeline:
    def __init__(self):
        pass

    def predict(self , features):
        try:
            preprocessor_path = os.path.join('artifacts' , 'preprocessor.pkl')
            model_path = os.path.join('artifacts' ,'model.pkl')

            preprocessor = load_object(preprocessor_path)
            model = load_object(model_path)

            data_scaled = preprocessor.transform(features)

            pred = model.predict(data_scaled)

            return pred

        except Exception as e:
            logging.info('Exception occured in prediction')
            raise CustomException(e,sys)     

class CustomData:

    def __init__(self,
                Airline: str,
                Source : str,
                Destination : str,
                Total_Stops : int,
                Journey_Day : int,
                Journey_Month:int,
                Dep_hour : int,
                Dep_min : int,
                Arrival_hour:int,
                Arrival_min:int,
                Duration_hour:int,
                Duration_min):
        
        self.Airline =  Airline
        self.Source = Source
        self.Destination  = Destination
        self.Total_Stops =  Total_Stops
        self.Journey_Day = Journey_Day
        self.Journey_Month  = Journey_Month
        self.Dep_hour = Dep_hour
        self. Dep_min  = Dep_min
        self.Arrival_hour =Arrival_hour
        self.Arrival_min =  Arrival_min
        self.Duration_hour = Duration_hour
        self.Duration_min =Duration_min
        

    def get_data_as_dataframe(self):
        try:
            custom_data_input_dict = {
                'Airline' : [self.Airline],
                'Source' : [self.Source],
                'Destination' : [self.Destination],
                'Total_Stops' : [self.Total_Stops],
                'Journey_Day' : [self.Journey_Day],
                'Journey_Month' : [self.Journey_Month],
                'Dep_hour' : [self.Dep_hour],
                'Dep_min' : [self.Dep_min],
                'Arrival_hour' : [self.Arrival_hour],
                'Arrival_min' : [self.Arrival_min],
                'Duration_hour' : [self.Arrival_hour - self.Dep_hour],
                'Duration_min' : [self.Arrival_min - self.Dep_min]
                
            }

            df = pd.DataFrame(custom_data_input_dict)
            logging.info('DataFrame Gathered')
            logging.info(f'Your new data set is{df}')
            return df
        
        except Exception as e:
            logging.info('Exception occured in prediction pipeline')
            raise CustomException(e ,sys)



