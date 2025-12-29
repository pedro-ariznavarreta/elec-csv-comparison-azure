from fastapi import FastAPI

app = FastAPI(title="elec-csv-comparison")

@app.get("/health")
def health():
    return {"status": "ok"}
