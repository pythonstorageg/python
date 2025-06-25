from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client['local']
# client = MongoClient("mongodb://gopi:gopi2iex5@10.88.0.39:27017/?authSource=admin&authMechanism=SCRAM-SHA-256")
# db = client["IEX"]

config_setup = {
    "minIO":{
        "endpoint": "http://10.88.0.39:9000",
        "access_key": "miniorootadmin",
        "secret_key": "m1n10@r00t@Psw",
        "source_bucket": "ml-flow-trails",
        },
    "MLFLOW":{
        "tracking_uri": "http://10.88.0.13:5003/",
        "experiment_name": "weather_data_demo1"},
    "weather_data":{
        "username":'greenko',
        "password":'GreenKo@ncmr9#',
        "url":"https://pdscloud.ncmrwf.gov.in:8443/api/v1/REdownload",
        "api_key":'S447KGpOXCJkBTwCiDB6N0bryryGRBij',
        },
    'mail_cred':{
        'userName': "g4caster@greenkogroup.com",              
        'appPasword': 'G4c@ster$pg#9',                        
        'to_recipients':['gopi.g@digitelenetworks.com'],
        'cc_recipients':['gopi.g@digitelenetworks.com']
        },
    'gemats_db':{
        "open_meteo_forecast": db['open_meteo_forecast'],
        "open_meteo_historical": db['open_meteo_historical'],
        "IMD_market_wise":db['IMD_market_wise']
    }
}
