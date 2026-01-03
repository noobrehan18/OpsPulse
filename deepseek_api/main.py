from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from config import settings
from models import ChatRequest, ChatResponse, HealthResponse
from service import deepseek_service


app = FastAPI(
    title="DeepSeek R1 API",
    description="API service for locally hosted DeepSeek R1 reasoning model",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Check API health status."""
    return HealthResponse(
        status="healthy",
        model=settings.MODEL_NAME
    )


@app.post("/v1/chat/completions", response_model=ChatResponse)
async def chat_completions(request: ChatRequest):
    """
    Generate a chat completion using locally hosted DeepSeek R1.
    
    DeepSeek R1 is a reasoning model that returns:
    - reasoning_content: The chain-of-thought reasoning process
    - content: The final answer/response
    """
    try:
        if request.stream:
            return StreamingResponse(
                deepseek_service.chat_stream(request),
                media_type="text/event-stream"
            )
        else:
            response = deepseek_service.chat(request)
            return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/v1/reason")
async def reason(request: ChatRequest):
    """
    Reasoning endpoint that emphasizes the chain-of-thought output.
    Returns both the reasoning process and final answer separately.
    """
    try:
        response = deepseek_service.chat(request)
        return {
            "reasoning": response.reasoning_content,
            "answer": response.content,
            "usage": response.usage
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=True
    )
