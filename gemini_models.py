import os
from pathlib import Path
from dotenv import load_dotenv
from google import genai

load_dotenv(Path(__file__).resolve().parent / ".env")

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

print("List of models that support generateContent:\n")

found = 0
for m in client.models.list():
    actions = getattr(m, "supported_actions", []) or []
    if "generateContent" in actions:
        print("-", m.name)
        found += 1

print(f"\nTrovati: {found}")
