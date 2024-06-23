import requests
import pandas as pd
import pymongo

data = pd.read_csv('Top100-US.csv', header = 0, delimiter=';')

url = "https://weatherapi-com.p.rapidapi.com/current.json"

headers = {
	"X-RapidAPI-Key": "22e6f756a5mshaeaa167d1642c7dp1d0bd2jsn7bb607c7195c",
	"X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}

MONGO_NAME = 'mongo'
MONGO_PORT = 27017
MONGO_DB = "BigData_Assignment1"
MONGO_COLL = "Top100-Weather"

myclient = pymongo.MongoClient(MONGO_NAME, MONGO_PORT)
mycol = myclient[MONGO_DB][MONGO_COLL]

for i in data.index:
    zipcode = str(data['Zip'][i])

    if len (zipcode) <= 5:
        s = ''
        for i in range (5-len(zipcode)):
            s += '0'
        zipcode = s + zipcode
    
    querystring = {"q":zipcode}

    response = requests.get(url, headers=headers, params=querystring)
    response_json = response.json()

    weather_condition = response_json['current']['condition']['text']
    local_time = response_json['location']['localtime']
    city = data['City'][i]
    
    mydict = { "zip": int(zipcode), 
              "city": city, 
              "created_at":local_time, 
              "weather":weather_condition}

    x = mycol.insert_one(mydict)