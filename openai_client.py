import os

from dotenv import load_dotenv
import openai

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")


def get_chat_completion(instructions, input, model="gpt-4o", temperature=0.2):
    """Return the assistant message from an OpenAI responses API call."""
    resp = openai.responses.create(
        model=model,
        instructions=instructions,
        input=input,
        temperature=temperature,
    )
    return resp.output_text.strip()
