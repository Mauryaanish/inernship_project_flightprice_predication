from flask import Flask , request , render_template,jsonify
from src.pipeline.prediction_pipeline import CustomData , PredictionPipeline
import pandas as pd



application = Flask(__name__)
app = application


@app.route('/')

def home_page():
    return render_template('index.html')

@app.route('/predict' , methods = ["GET" , "POST"])

def predict_datapoint():
    if request.method =='GET':
        return render_template('home.html')
        
    else:
       
        data = CustomData(
        Airline=str(request.form.get('Airline')),
        Source = str(request.form.get('Srouce')),
        Destination = str(request.form.get('Destination')),
        Total_Stops = str(request.form.get('Total_Stops')),
         # Date_of_Journey
        #date_dep = request.form["Dep_Time"]
        Journey_Day = int(pd.to_datetime(request.form["Dep_Time"], format="%Y-%m-%dT%H:%M").day),
        Journey_Month = int(pd.to_datetime(request.form["Dep_Time"], format ="%Y-%m-%dT%H:%M").month),
        # print("Journey Date : ",Journey_day, Journey_month)

        # Departure
        Dep_hour = int(pd.to_datetime(request.form["Dep_Time"], format ="%Y-%m-%dT%H:%M").hour),
        Dep_min = int(pd.to_datetime(request.form["Dep_Time"], format ="%Y-%m-%dT%H:%M").minute),
        # print("Departure : ",Dep_hour, Dep_min)

        # Arrival
        #date_arr = request.form["Arrival_Time"]
        Arrival_hour = int(pd.to_datetime(request.form["Arrival_Time"], format ="%Y-%m-%dT%H:%M").hour),
        Arrival_min = int(pd.to_datetime(request.form["Arrival_Time"], format ="%Y-%m-%dT%H:%M").minute),
        # print("Arrival : ", Arrival_hour, Arrival_min)

        # Duration
        Duration_hour =('Duration_hour'),
        Duration_min =('Duration_min')
        # print("Duration : ", dur_hour, dur_min)

        )

        
        final_new_data = data.get_data_as_dataframe()
        predict_pipeline = PredictionPipeline()
        pred = predict_pipeline.predict(final_new_data)

        results = round(pred[0],2)

        return render_template('home.html' , final_result =results)
    




if __name__ == '__main__':
    app.run(host = '0.0.0.0' , debug = True , port = 5000)
