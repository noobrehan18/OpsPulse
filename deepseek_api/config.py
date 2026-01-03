import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Local model server URL (Ollama, vLLM, llama.cpp, etc.)
    LOCAL_MODEL_URL: str = os.getenv("LOCAL_MODEL_URL", "http://localhost:11434/v1")
    
    # Model name as registered in your local server
    MODEL_NAME: str = os.getenv("MODEL_NAME", "deepseek-r1")
    
    # Server settings
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))


settings = Settings()
