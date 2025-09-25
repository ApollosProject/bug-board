import json

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Initialize OpenAI client for chat-based function calling
client = OpenAI()


def get_chat_function_call(
    instructions,
    user_input,
    functions,
    function_call_name,
    model="gpt-5",
    temperature=None,
):
    """Call OpenAI chat completion with function calling and return the parsed JSON arguments."""
    # Ensure functions is a list of function specifications
    if not isinstance(functions, list):
        functions = [functions]
    request_kwargs = {
        "model": model,
        "messages": [
            {"role": "system", "content": instructions},
            {"role": "user", "content": user_input},
        ],
        "functions": functions,
        "function_call": {"name": function_call_name},
    }
    if temperature is not None:
        request_kwargs["temperature"] = temperature

    response = client.chat.completions.create(**request_kwargs)
    arguments = response.choices[0].message.function_call.arguments
    return json.loads(arguments)
