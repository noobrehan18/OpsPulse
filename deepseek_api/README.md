# DeepSeek R1 API Service

A FastAPI-based service for serving DeepSeek R1, a reasoning model that provides chain-of-thought explanations.

## Features

- **Chat Completions**: Standard chat completion endpoint compatible with OpenAI format
- **Reasoning Output**: Access to DeepSeek R1's chain-of-thought reasoning
- **Streaming Support**: Real-time streaming responses via SSE
- **Token Usage Tracking**: Detailed token usage including reasoning tokens

## Setup

1. **Install dependencies**:
   ```bash
   cd deepseek_api
   pip install -r requirements.txt
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env and add your DeepSeek API key
   ```

3. **Run the server**:
   ```bash
   python main.py
   ```
   
   Or with uvicorn directly:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

## API Endpoints

### Health Check
```
GET /health
```

### Chat Completions
```
POST /v1/chat/completions
```

**Request Body**:
```json
{
  "messages": [
    {"role": "user", "content": "What is 25 * 47?"}
  ],
  "temperature": 0.7,
  "max_tokens": 4096,
  "stream": false
}
```

**Response**:
```json
{
  "id": "chatcmpl-xxx",
  "model": "deepseek-reasoner",
  "reasoning_content": "Let me calculate 25 * 47 step by step...",
  "content": "25 × 47 = 1,175",
  "usage": {
    "prompt_tokens": 10,
    "completion_tokens": 150,
    "total_tokens": 160,
    "reasoning_tokens": 120
  }
}
```

### Reasoning Endpoint
```
POST /v1/reason
```

Returns reasoning and answer separately:
```json
{
  "reasoning": "Step-by-step reasoning process...",
  "answer": "Final answer",
  "usage": {...}
}
```

## Streaming

Set `"stream": true` in the request to receive Server-Sent Events:

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"messages": [{"role": "user", "content": "Explain quantum computing"}], "stream": true}'
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DEEPSEEK_API_KEY` | - | Your DeepSeek API key (required) |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | DeepSeek API base URL |
| `MODEL_NAME` | `deepseek-reasoner` | Model to use (deepseek-reasoner for R1) |
| `HOST` | `0.0.0.0` | Server host |
| `PORT` | `8000` | Server port |

## API Documentation

Once running, access the interactive API docs at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
