
import requests
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def weather_etl(user_input):
    from datetime import datetime
    lat=17.3753
    lon=78.4744
    # start=datetime.date(2023,8,20)
    # end=datetime.date(2023,8,21)
    api_key='70de1cd6a4b552df328b9d37c66b7892'
    weather_data=requests.get(f"https://history.openweathermap.org/data/2.5/history/timemachine?lat={lat}&lon={lon}&dt=606348800&type=hour&appid={api_key}")
    weather = weather_data.json()
    '''
    
    weather_data = {
        'temp': weather['main']['temp'],
        'feels_like': weather['main']['feels_like'],
        'temp_min': weather['main']['temp_min'],
        'temp_max': weather['main']['temp_max'],
        'pressure': weather['main']['pressure'],
        'humidity': weather['main']['humidity']
    }
http://history.openweathermap.org/data/2.5/history/city?id=2885679&type=hour&appid=70de1cd6a4b552df328b9d37c66b7892
    # Create a DataFrame from the weather data
    df1=pd.DataFrame(weather_data, index=[weather['name']])
    '''
    return weather_data
    return df1.to_csv('s3://airflow-bucket-rajender/{}.csv'.format(user_input), index=False)
print(weather_etl('Hyderabad'))