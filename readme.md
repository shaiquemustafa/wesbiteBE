
## 🚀 Getting Started
Follow these instructions to get the project set up and running on your local machine for development and testing purposes.

##Prerequisites
Python 3.8 or newer

pip (Python package installer)

## ⚙️ Setup & Installation
1. Clone the Repository

```
git clone [elrond-stock-analyzer-be](https://github.com/mab-007/elrond-stock-analyzer-be)
cd elrond-stock-analyzer-be
```

2. Set Up Environment Variables
   Create a file named `.env` in the root of the project directory and add your OpenAI API key to it:
   ```
   OPENAI_API_KEY="your_secret_api_key_here"
   ```
3. Create and Activate a Virtual Environment
   A virtual environment (venv) is highly recommended to keep project dependencies isolated.
   If you don't have a venv directory yet, create one:
   ```
   python3 -m venv venv
   ```
   This command creates a new folder named venv in your project directory.

   Activate the virtual environment:

      On macOS / Linux:
      ```
      source venv/bin/activate
      ```
      On Windows:
      ```
      .\venv\Scripts\activate
      ```
    You'll know the environment is active when you see (venv) at the beginning of your terminal prompt.

4. Install Required Packages

    Install all the necessary libraries and packages listed in the requirements.txt file.
    ```
    pip install -r requirements.txt
    ```
## ▶️ Running the Application
With your virtual environment active and dependencies installed, you can start the API server.

This project uses Uvicorn, an ASGI server, to run the application.
To make the server accessible from your local network (or from within a Docker container), you must specify the host.
```
uvicorn api:app --reload
```
api: Refers to the Python file api.py.

app: Refers to the FastAPI instance object inside api.py.

--reload: This flag enables auto-reload, so the server will restart automatically whenever you make changes to the code.

Your API should now be running locally at https://www.google.com/search?q=http://127.0.0.1:8080.

## 🌐 Exposing a Public Endpoint with Ngrok (Optional)
If you need to expose your local API to the internet (for testing webhooks, sharing with a teammate, etc.), you can use ngrok.

1. Install Ngrok

  Go to the Ngrok official website and download the correct version for your operating system.
  
  Unzip the package and follow their instructions to add your authtoken (a one-time setup).

2. Run Ngrok

  Make sure your Uvicorn server is running.
  
  Open a new, separate terminal window.
  
  Run the following command to point ngrok to your local server's port (which is 8080 by default for Uvicorn).
  ```
  ngrok http 8080
  ````
  Ngrok will provide you with a public URL (e.g., https://random-string.ngrok.io) that tunnels directly to your local application.

### 🎉 AND ENJOY!
Your API is now fully set up. Happy coding!
<!-- STEP 1: source /Users/mab/Desktop/papumpare_predictions/venv/bin/activate
STEP 2: uvicorn api:app --reload


AND ENJOY -->
