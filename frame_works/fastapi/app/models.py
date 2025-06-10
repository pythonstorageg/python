from pydantic import BaseModel
from enum import Enum
from datetime import date

class Albums(BaseModel):
    title:str
    created_date:date

class pre_defined_names(Enum):
    Ramesh = "ramesh"
    Suresh = "paramesh"
    Mahesh = "mahesh"

class pre_defined_ids(Enum):
    one = "1"
    tw0 = "2"
    three = "3"

class BAND(BaseModel):
    id:int
    name:str
    age:int
    albums:list[Albums] = []