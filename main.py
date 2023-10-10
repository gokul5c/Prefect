
from typing import Union
import sqlalchemy as db
import os
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from sqlalchemy import text
from prefect import flow    

import json
app = FastAPI()
env_path = '.env'
load_dotenv(dotenv_path=env_path)
def connect():
    connection_url = db.engine.URL.create(
                        drivername="clickhouse",
                        username=os.getenv("CH_USER"),
                        password=os.getenv("CH_PASSWORD"),
                        host=os.getenv("CH_HOST"),
                        database="study",
                    )
    return connection_url


# @app.get("/")\
@flow(name='click_connect')
def read_root():
    try:
        connection_url = connect()
        engine = db.create_engine(connection_url)
        connection = engine.connect()
        data = pd.DataFrame(connection.execute('''select id,replace((JSONExtractArrayRaw(simpleJSONExtractRaw(assumeNotNull(REPLACE(rules, '\\\\' , '')),'list'))[1]),'"','') as modality,
		replace((JSONExtractArrayRaw(simpleJSONExtractRaw(assumeNotNull(REPLACE(rules, '\\\\' , '')),'list'))[-1]),'"','') as organ from study.Studies limit 2'''))
        # console.log(data)
        print(data)
        data=data.to_json(orient="records", date_format='iso', date_unit='s')
        data = json.loads(data)
        print(data)
        dict={}
        for i in range(0,len(data)):
            dict[i]=data[i]
        # data.
        print(dict)
        return dict
        print('hello')
    except Exception as e:  
        print('error', e)

