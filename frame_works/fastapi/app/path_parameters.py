from fastapi import FastAPI, HTTPException
from models import pre_defined_names, BAND, pre_defined_ids

app = FastAPI()

BANDS = [
    {"id":1,"name":"ramesh","age":25},
    {"id":2,"name":"paramesh","age":30,
     "albums":[{"title":"so and so","created_date":'1971-01-01'}]},
   {"id":3,"name":"mahesh","age":35}
]

@app.get("/bands")
async def root() -> list[dict]:
    return BANDS

@app.get("/bands/{name}")
async def get_item(name:pre_defined_names):
    return [b for b in BANDS if b['name'].lower()==name.value] 

@app.get("/pydantic")
async def root():
    return [BAND(**b) for b in BANDS]

@app.get("/pydantic/{id}")
async def root(id:pre_defined_ids):
    print("yes")
    return [BAND(**b) for b in BANDS if b['id']==int(id.value)]

