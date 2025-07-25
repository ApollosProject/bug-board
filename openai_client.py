import json

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Initialize OpenAI client for chat-based function calling
client = OpenAI()


def get_chat_function_call(
    instructions, input, functions, function_call_name, model="gpt-4o", temperature=0.2
):
    """Call OpenAI chat completion with function calling and return the parsed JSON arguments."""
    # Ensure functions is a list of function specifications
    if not isinstance(functions, list):
        functions = [functions]
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": instructions},
            {"role": "user", "content": input},
        ],
        functions=functions,
        function_call={"name": function_call_name},
        temperature=temperature,
    )
    arguments = response.choices[0].message.function_call.arguments
    return json.loads(arguments)
