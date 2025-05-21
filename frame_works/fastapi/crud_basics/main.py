from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Input_data(BaseModel):
    name:str
    mny:int

@app.get("/")
def getting_statement():
    return {"first statement"}

@app.get("/{name}")
def getting_statement(name=str):
    return {f"Hi {name}"}

@app.post("/")
def analyzingdata(dataReceived:Input_data):
    data = dataReceived.dict()
    msg = f"Hi {data['name']}, Received amount of {data['mny']}"
    return {"msg":msg}

