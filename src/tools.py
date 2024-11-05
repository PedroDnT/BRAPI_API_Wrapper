import json
import sys
import os

# Add the project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from brapi_wrapper import *

# Load the JSON schema
schema_path = os.path.join(os.path.dirname(__file__), '..', 'openai_tools_schema.json')
with open(schema_path) as f:
    tools_schema = json.load(f)

# Create a mapping from function names to actual functions
FUNCTIONS_MAP = {func['name']: globals()[func['name']] for func in tools_schema['functions']}

def execute_function(function_name, parameters):
    func = FUNCTIONS_MAP.get(function_name)
    if not func:
        raise ValueError(f"Function '{function_name}' not found.")
    return func(**parameters) 