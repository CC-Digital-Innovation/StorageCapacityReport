import uvicorn

if __name__ == "__main__":
    uvicorn.run("nocodb-test:app", host="0.0.0.0", port = 8000)