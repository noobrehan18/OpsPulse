from typing import Optional, List
from pydantic import BaseModel, Field


class Message(BaseModel):
    role: str = Field(..., description="Role of the message sender: 'user', 'assistant', or 'system'")
    content: str = Field(..., description="Content of the message")


class ChatRequest(BaseModel):
    messages: List[Message] = Field(..., description="List of messages in the conversation")
    temperature: Optional[float] = Field(default=0.7, ge=0.0, le=2.0, description="Sampling temperature")
    max_tokens: Optional[int] = Field(default=4096, ge=1, le=8192, description="Maximum tokens to generate")
    stream: Optional[bool] = Field(default=False, description="Whether to stream the response")


class ReasoningContent(BaseModel):
    reasoning: Optional[str] = Field(None, description="Chain-of-thought reasoning from DeepSeek R1")
    content: str = Field(..., description="Final response content")


class ChatResponse(BaseModel):
    id: str = Field(..., description="Unique response ID")
    model: str = Field(..., description="Model used for generation")
    reasoning_content: Optional[str] = Field(None, description="Chain-of-thought reasoning")
    content: str = Field(..., description="Generated response content")
    usage: dict = Field(..., description="Token usage statistics")


class HealthResponse(BaseModel):
    status: str = Field(..., description="Service health status")
    model: str = Field(..., description="Configured model name")
