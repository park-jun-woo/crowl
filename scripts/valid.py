import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig, AutoConfig
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

app = FastAPI()

model_id = "meta-llama/Llama-3.1-8B"

config = AutoConfig.from_pretrained(model_id)
config.attn_implementation = "flash_attention_2"

bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_use_double_quant=True,
    bnb_4bit_compute_dtype=torch.float16,
)

tokenizer = AutoTokenizer.from_pretrained(model_id)
tokenizer.pad_token = tokenizer.eos_token

# 모델 로딩 상태 플래그
model_loaded = False

@app.on_event("startup")
def load_model():
    global model, model_loaded
    print("🚀 모델 로딩 시작...")
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        config=config,
        quantization_config=bnb_config,
        device_map="auto",
        max_memory={0: "14GiB", "cpu": "16GiB"}
    )
    model_loaded = True
    print("✅ 모델 로딩 완료!")

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
Is the above text an actual news article or an error page?  
Respond with exactly one word from:
- article
- error
- unknown

[Answer]
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
#        answers = [text.split("[Answer]")[-1].strip().split()[0] for text in generated_texts]

        return {"answers": generated_texts}

    except Exception as e:
        print(f"🔥 추론 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("valid:app", host="127.0.0.1", port=8000, reload=False)
