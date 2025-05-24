import os
import json
import httpx
import asyncio
from dotenv import load_dotenv, find_dotenv

# Load environment variables
load_dotenv(find_dotenv())

# Constants
SNOWFLAKE_ACCOUNT_URL = os.getenv("SNOWFLAKE_ACCOUNT_URL")
SNOWFLAKE_PAT = os.getenv("SNOWFLAKE_PAT")
SEMANTIC_MODEL_FILE = os.getenv("SEMANTIC_MODEL_FILE")
CORTEX_SEARCH_SERVICE = os.getenv("CORTEX_SEARCH_SERVICE")

# Headers for API requests
API_HEADERS = {
    "Authorization": f"Bearer {SNOWFLAKE_PAT}",
    "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    "Content-Type": "application/json",
    "Accept": "text/event-stream",
}

async def process_sse_response(resp: httpx.Response):
    """Process the SSE response from the Cortex Agent."""
    async for line in resp.aiter_lines():
        if line.strip():
            if line.startswith("data: "):
                data = line[6:]  # Remove "data: " prefix
                if data == "[DONE]":
                    print("Stream completed")
                    break
                try:
                    parsed_data = json.loads(data)
                    print(f"Received data: {json.dumps(parsed_data, indent=2)}")
                except json.JSONDecodeError:
                    print(f"Could not parse line as JSON: {data}")

async def test_cortex_agent():
    """Test the Cortex Agent API with a sample query."""
    # Sample payload
    payload = {
        "model": "claude-3-5-sonnet",
        "response_instruction": "You are a helpful AI assistant.",
        "experimental": {},
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "Analyst1"}},
            {"tool_spec": {"type": "cortex_search", "name": "Search1"}},
            {"tool_spec": {"type": "sql_exec", "name": "sql_execution_tool"}},
        ],
        "tool_resources": {
            "Analyst1": {"semantic_model_file": SEMANTIC_MODEL_FILE},
            "Search1": {"name": CORTEX_SEARCH_SERVICE},
        },
        "tool_choice": {"type": "auto"},
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "Show me the total sales by region"}]}
        ],
    }

    url = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/agent:run"
    
    print(f"Making request to: {url}")
    print(f"With payload: {json.dumps(payload, indent=2)}")
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, json=payload, headers=API_HEADERS)
            
            if response.status_code == 200:
                print("Successfully connected to the API")
                # For streaming responses
                async with client.stream("POST", url, json=payload, headers=API_HEADERS) as stream_response:
                    await process_sse_response(stream_response)
            else:
                error_body = response.text
                try:
                    error_json = response.json()
                    print(f"Error {response.status_code}:")
                    print(json.dumps(error_json, indent=2))
                except:
                    print(f"Error {response.status_code}:")
                    print(error_body)
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    # Check if required environment variables are set
    required_vars = ["SNOWFLAKE_ACCOUNT_URL", "SNOWFLAKE_PAT", "SEMANTIC_MODEL_FILE", "CORTEX_SEARCH_SERVICE"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please make sure these are set in your .env file")
        exit(1)
    
    # Print environment variables for debugging (excluding PAT)
    print("Using configuration:")
    print(f"SNOWFLAKE_ACCOUNT_URL: {SNOWFLAKE_ACCOUNT_URL}")
    print(f"SEMANTIC_MODEL_FILE: {SEMANTIC_MODEL_FILE}")
    print(f"CORTEX_SEARCH_SERVICE: {CORTEX_SEARCH_SERVICE}")
        
    print("\nStarting Cortex Agent API test...")
    asyncio.run(test_cortex_agent()) 