from flask import Flask , request , render_template,jsonify
from src.pipeline.prediction_pipeline import CustomData , PredictionPipeline
import calendar



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
            Airline = str(request.form.get('Airline')),
            Source = str(request.form.get('Source')),
            Destination = str(request.form.get('Destination')),
            Total_Stops=  str(request.form.get('Total_Stops')),
            Jounery_Day=  int(request.form.get('Journery')),
            Jounery_Month= int(request.form.get('Jounery_Month')),
            Dep_hour= int(request.form.get('Dep_hour')),
            Dep_min= int(request.form.get('Dep_min')),
            Arrival_hour=int(request.form.get('Arrival_hour')),
            Arrival_min= int(request.form.get('Arrival_min')),
            Duration_hour=int(request.form.get('Duration_hour')),
            Duration_min=int(request.form.get('Duration_min'))

        )

        
        final_new_data = data.get_data_as_dataframe()
        predict_pipeline = PredictionPipeline()
        pred = predict_pipeline.predict(final_new_data)

        results = round(pred[0],2)

        return render_template('home.html' , final_result =results)
    




if __name__ == '__main__':
    app.run(host = '0.0.0.0' , debug = True , port = 5500)
