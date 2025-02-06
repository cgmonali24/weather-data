import requests
from airflow.models import Variable

API_KEY = Variable.get("weather_api_key")
API_BASE_URL = Variable.get('api_base_url')
API_PARAMS_GROUP= Variable.get('api_params_unitGroup')
API_PARAMS_INCLUDE= Variable.get('api_params_include')

def fetch_weather_data(**kwargs):
    """
    Fetches weather data for a given date range.
    Args:
        **kwargs: Arbitrary keyword arguments. Expects 'params' dictionary with 'start-date' and 'end-date' keys.
    Returns:
        list: A list of dictionaries containing weather data for each day in the date range, or None if an error occurs.
    """
    start_date = kwargs['params']['start-date']
    end_date = kwargs['params']['end-date']
    
    req_url = f"{API_BASE_URL}/{start_date}/{end_date}?unitGroup={API_PARAMS_GROUP}&include={API_PARAMS_INCLUDE}&key={API_KEY}&contentType=json"
    try:
        response = requests.get(req_url)
        response.raise_for_status()
        weather_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

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