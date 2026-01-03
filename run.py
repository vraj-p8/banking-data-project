"""
Application entry point for running the FastAPI server
"""
import os
import uvicorn
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        reload=os.getenv("ENVIRONMENT") != "production",
        log_level="info"
    )
