import json
import os
import pandas as pd

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import status
from fastapi.responses import JSONResponse

from pydantic import BaseModel

from sqlalchemy import create_engine
from sqlalchemy.sql import text

# Base class for number
class Number(BaseModel):
    number: int # recieved number

# Creating PostgreSQL engine
engine = create_engine(
    f"postgresql://{os.environ['user']}:{os.environ['password']}@{os.environ['host']}:{os.environ['port']}/{os.environ['database']}"
)

# Creating app
server = FastAPI()

### -- METHODS -- ###

# Parsing data func
def parse_data(data):
    res         = data.to_json(orient="records")
    parsed_data = json.loads(res)
    
    return parsed_data

# Processing data func
@server.post("/process_data/")
async def process_data(request: Number):
    number = request.number

    pgs_cnt = engine.connect()
    pgs_cnt.execute(
        text("CREATE TABLE IF NOT EXISTS Numbers (number INT);")
    )

    query = f"SELECT * FROM Numbers WHERE number = {number}"
    result = pd.read_sql(query, pgs_cnt)

    # Exceptions
    if len(result > 0):
        raise HTTPException(
            status_code=406, detail=f"Number {number} is already exists"
        )

    query = f"SELECT * FROM Numbers WHERE number = {number + 1};"
    result = pd.read_sql(query, pgs_cnt)

    # Exceptions
    if len(result) > 0:
        raise HTTPException(
            status_code=406, detail=f"Number {number} + 1 is already exists"
        )

    pgs_cnt.execute(
        text(f"INSERT INTO Numbers (number) VALUES ({number});")
    )

    pgs_cnt.close()

    return JSONResponse(
        status_code=status.HTTP_200_OK, content=f"Number {number} inserted"
    )

# Deleting data func
@server.post("/delete_data/")
async def delete_data():
    pgs_cnt = engine.connect()

    pgs_cnt.execute(text("DELETE FROM Numbers;"))
    
    pgs_cnt.close()

    return JSONResponse(status_code=status.HTTP_200_OK, content="Data erased")

# Getting data func
@server.get("/get_data/")
async def get_data():
    pgs_cnt = engine.connect()

    data = pd.read_sql("SELECT * FROM Numbers", pgs_cnt)
    
    pgs_cnt.close()
    
    return JSONResponse(status_code=status.HTTP_200_OK, content=parse_data(data))

#####################