import os
import sys


api_key = os.getenv("GEMINI_API_KEY")
print("\nGEMINI_API_KEY presente:", bool(api_key))
if api_key:
    print("Prefix:", api_key[:6] + "..." + api_key[-4:])

print("\nTest import google.genai")
try:
    from google import genai
    print("google.genai importato correttamente")
except Exception as e:
    print("Errore import google.genai")
    print(type(e).__name__, e)
    sys.exit(1)

print("\nTest chiamata Gemini")
try:
    client = genai.Client(api_key=api_key)

    response = client.models.generate_content(
        model="models/gemini-flash-lite-latest",
        contents="Rispondi solo con la parola: OK"
    )

    print("Gemini risponde:", response.text)
except Exception as e:
    print("Errore chiamata Gemini")
    print(type(e).__name__)
    print(e)
