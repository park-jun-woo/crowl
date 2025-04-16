import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, AutoConfig
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

app = FastAPI()

model_id = "meta-llama/Llama-3.1-8B"

config = AutoConfig.from_pretrained(model_id)
config.attn_implementation = "flash_attention_2"

tokenizer = AutoTokenizer.from_pretrained(model_id)
tokenizer.pad_token = tokenizer.eos_token

model_loaded = False

@app.on_event("startup")
def load_model():
    global model, model_loaded
    print("🚀 모델 로딩 시작 (FP16)...")
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        config=config,
        torch_dtype=torch.float16,  # FP16 로딩
        device_map="auto",
        max_memory={0: "14GiB", "cpu": "16GiB"}
    )
    model_loaded = True
    print("✅ 모델 로딩 완료 (FP16)!")

class PromptRequest(BaseModel):
    texts: list[str]

@app.get("/health")
async def health():
    if model_loaded:
        return {"status": "ok"}
    else:
        raise HTTPException(status_code=503, detail="Model loading...")

@app.post("/infer")
async def infer(request: PromptRequest):
    if not model_loaded:
        raise HTTPException(status_code=503, detail="Model loading...")

    try:
        prompts = [
            f"""[text]
{text}

[Question]
Classify the text above strictly into one of the following labels:
- article
- error
- unknown

Label:
""" for text in request.texts]

        inputs = tokenizer(prompts, return_tensors="pt", padding=True, truncation=True).to(model.device)

        with torch.inference_mode():
            outputs = model.generate(
                **inputs,
                max_new_tokens=5,
                do_sample=False,
                repetition_penalty=1.2,
                pad_token_id=tokenizer.eos_token_id,
                eos_token_id=tokenizer.eos_token_id
            )

        generated_texts = tokenizer.batch_decode(outputs, skip_special_tokens=True)

        return {"answers": generated_texts}

    except Exception as e:
        print(f"🔥 추론 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("valid:app", host="127.0.0.1", port=8000, reload=False)
