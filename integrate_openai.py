import openai
import json
import requests
from dotenv import load_dotenv
import os
import os.path

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Debug: Verify if API key is loaded
if openai.api_key:
    print("OpenAI API Key loaded successfully.")
else:
    raise ValueError("Failed to load OpenAI API Key. Please check your .env file.")

# Adjust the path to openai_tools_schema.json
schema_path = os.path.join(os.path.dirname(__file__), 'openai_tools_schema.json')

# Verify if the schema file exists
if not os.path.exists(schema_path):
    raise FileNotFoundError(f"Schema file not found at {schema_path}")

# Load the functions schema
with open(schema_path) as f:
    functions = json.load(f)['functions']

# Example prompt
prompt = "Fetch the quote for ticker PETR4 over the past month."

# Create a chat completion request
try:
    response = openai.ChatCompletion.create(
        model="gpt-4",  # Use a valid model name, e.g., "gpt-4" or "gpt-3.5-turbo"
        messages=[{"role": "user", "content": prompt}],
        functions=functions,
        function_call="auto",
    )
except Exception as e:
    raise RuntimeError(f"Failed to create ChatCompletion: {e}")

message = response['choices'][0]['message']

if message.get("function_call"):
    function_name = message["function_call"]["name"]
    parameters = json.loads(message["function_call"]["arguments"])
    
    # Debug: Print function call details
    print(f"Function to call: {function_name}")
    print(f"Parameters: {parameters}")
    
    # Call your FastAPI endpoint
    try:
        api_response = requests.post("http://localhost:8000/execute", json={
            "function_name": function_name,
            "parameters": parameters
        })
    except Exception as e:
        raise ConnectionError(f"Failed to connect to FastAPI server: {e}")
    
    if api_response.status_code == 200:
        result = api_response.json().get("result")
    else:
        result = {"error": f"Failed to fetch data: {api_response.text}"}
    
    # Debug: Print API response
    print(f"API Response: {result}")
    
    # Send the result back to OpenAI
    try:
        completion = openai.ChatCompletion.create(
            model="gpt-4",  # Use the same valid model
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
                message,
                {"role": "function", "name": function_name, "content": json.dumps(result)}
            ]
        )
    except Exception as e:
        raise RuntimeError(f"Failed to create ChatCompletion with function result: {e}")

    answer = completion['choices'][0]['message']['content']
    print(answer)
else:
    print(message['content']) 