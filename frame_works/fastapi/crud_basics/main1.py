from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

app = FastAPI()

class Input_data(BaseModel):
    id:int
    name:str
    status:bool

class Modifying_data(BaseModel):
    name:str

database = [{
    "id":1,
    "name":"x",
    "status":True
},
{
    "id":2,
    "name":"y",
    "status":False
},
{
    "id":3,
    "name":"z",
    "status":True
}
]

def getting_id_data(id):
    for i in database:
        if i['id'] == id:
            return i
        
def modifying_data(id,data):
    for i in database:
        if i['id'] == int(id):
            i['name'] = data['name']
      
@app.get("/")
def getting_statement():
    return database

@app.get("/{id}")
def getting_statement(id=int):
    data = getting_id_data(int(id))
    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f'{id} not available')
    return data

@app.post("/",status_code=status.HTTP_201_CREATED)
def adding_data(dataReceived:Input_data):
    data = dataReceived.dict()
    database.append(data)
    return {"msg":"Data added successfully","data":database}

@app.patch("/{id}")
def updating_data(dataReceived:Modifying_data,id=int):
    data = dataReceived.dict()
    modifying_data(id,data)
    return database


