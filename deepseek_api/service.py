import httpx
from typing import AsyncGenerator
import json

from config import settings
from models import ChatRequest, ChatResponse


class DeepSeekService:
    def __init__(self):
        self.base_url = settings.LOCAL_MODEL_URL.rstrip('/')
        self.model = settings.MODEL_NAME
        self.timeout = httpx.Timeout(300.0)  # 5 min timeout for reasoning

    def chat(self, request: ChatRequest) -> ChatResponse:
        """
        Send a chat request to locally hosted DeepSeek R1.
        """
        messages = [{"role": m.role, "content": m.content} for m in request.messages]
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "stream": False
        }
        
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                f"{self.base_url}/chat/completions",
                json=payload
            )
            response.raise_for_status()
            data = response.json()
        
        choice = data["choices"][0]
        message = choice["message"]
        
        # DeepSeek R1 may include reasoning_content
        reasoning_content = message.get("reasoning_content")
        
        usage = data.get("usage", {})
        
        return ChatResponse(
            id=data.get("id", "local"),
            model=data.get("model", self.model),
            reasoning_content=reasoning_content,
            content=message.get("content", ""),
            usage={
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
                "total_tokens": usage.get("total_tokens", 0),
                "reasoning_tokens": usage.get("reasoning_tokens", 0)
            }
        )

    async def chat_stream(self, request: ChatRequest) -> AsyncGenerator[str, None]:
        """
        Stream chat response from locally hosted DeepSeek R1.
        Yields Server-Sent Events (SSE) formatted chunks.
        """
        messages = [{"role": m.role, "content": m.content} for m in request.messages]
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "stream": True
        }
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/chat/completions",
                json=payload
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        chunk_data = line[6:]  # Remove "data: " prefix
                        if chunk_data == "[DONE]":
                            yield "data: [DONE]\n\n"
                            break
                        
                        try:
                            chunk = json.loads(chunk_data)
                            if chunk.get("choices"):
                                delta = chunk["choices"][0].get("delta", {})
                                data = {
                                    "id": chunk.get("id", "local"),
                                    "model": chunk.get("model", self.model),
                                    "reasoning_content": delta.get("reasoning_content"),
                                    "content": delta.get("content")
                                }
                                yield f"data: {json.dumps(data)}\n\n"
                        except json.JSONDecodeError:
                            continue


# Singleton instance
deepseek_service = DeepSeekService()
