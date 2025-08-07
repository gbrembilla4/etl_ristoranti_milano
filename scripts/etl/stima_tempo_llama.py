import time
import requests
import json

OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"

sample_prompts = [
    "Dato il seguente piatto di tipo pizza con questi ingredienti: pomodoro, mozzarella, basilico, stima approssimativamente quante calorie ha e se può essere considerato healthy. Rispondi nel formato JSON: {\"calorie\": numero, \"healthy\": \"healthy/unhealthy\"}",
    "Dato il seguente piatto di tipo hamburger con questi ingredienti: pane, carne di manzo, cheddar, stima approssimativamente quante calorie ha e se può essere considerato healthy. Rispondi nel formato JSON: {\"calorie\": numero, \"healthy\": \"healthy/unhealthy\"}"
]

def llama_infer(prompt):
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False
    }
    response = requests.post(OLLAMA_API_URL, json=payload)
    return response.json()['response']

# Benchmark
start_time = time.time()
for prompt in sample_prompts:
    response = llama_infer(prompt)
    print(response.strip())
end_time = time.time()

avg_time_per_prompt = (end_time - start_time) / len(sample_prompts)
estimated_time_for_50k = avg_time_per_prompt * 50000 / 60  # in minutes

print(f"\nTempo medio per piatto: {avg_time_per_prompt:.2f} sec")
print(f"Tempo stimato per 50k piatti: {estimated_time_for_50k/60:.2f} ore")
