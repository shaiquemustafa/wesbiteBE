import uvicorn

if __name__ == "__main__":
    """
    This script provides an alternative way to run the FastAPI application.
    You can execute this file directly with Python.
    
    Usage: python run.py
    """
    uvicorn.run(
        "api:app",       # The import string for the app
        host="0.0.0.0",    # The host to bind to
        port=8080,         # The port to listen on
        reload=True        # Set to False for production
    )
