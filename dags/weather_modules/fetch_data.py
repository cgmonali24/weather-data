import requests
from airflow.models import Variable

API_KEY = Variable.get("weather_api_key")

def fetch_weather_data(**kwargs):
    start_date = kwargs['params']['start-date']
    end_date = kwargs['params']['end-date']
    
    req_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/bengaluru/{start_date}/{end_date}?unitGroup=metric&include=days&key={API_KEY}&contentType=json"
    response = requests.get(req_url)
    weather_data = response.json()

    weather_data_list = []
    for day in weather_data["days"]:
        weather_data_list.append({
            "datetime": day["datetime"],
            "temp": day["temp"],
            "description": day["description"],
            "humidity": day["humidity"],
        })
    weather_data = weather_data_list


    return weather_data