from fastapi import FastAPI  # Here FastAPI is the class

app = FastAPI()   # app is the variable which acts as object of FastAPI class

@app.get("/greet")
def reading_path():
    return {"meessage":"Hello world"}

# To run the fastAPI we have two modes like dev (development mode) and run (production mode)
# fastapi dev script_name.py 

