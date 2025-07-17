import os

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_chat_completion(instructions, input, model="gpt-4o", temperature=0.2):
    """Return the assistant message from an OpenAI responses API call."""
    resp = _client.responses.create(
        model=model,
        instructions=instructions,
        input=input,
        temperature=temperature,
    )
    return resp.output_text.strip()
