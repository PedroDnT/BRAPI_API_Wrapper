from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from .tools import execute_function
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

app = FastAPI()

class FunctionCall(BaseModel):
    function_name: str
    parameters: dict

@app.post("/execute")
async def execute(function_call: FunctionCall):
    try:
        result = execute_function(function_call.function_name, function_call.parameters)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 