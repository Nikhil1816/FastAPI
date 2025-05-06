from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from uuid import uuid4
from tasks import process_csv
from typing import Dict
import os

app = FastAPI()

# In-memory storage
job_status: Dict[str, str] = {}
job_results: Dict[str, dict] = {}

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    job_id = str(uuid4())
    file_path = os.path.join(UPLOAD_DIR, f"{job_id}.csv")

    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)

    job_status[job_id] = "processing"
    background_tasks.add_task(process_csv, job_id, file_path, job_status, job_results)

    return {"job_id": job_id}

@app.get("/status/{job_id}")
def get_status(job_id: str):
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job ID not found")
    return {"job_id": job_id, "status": job_status[job_id]}

@app.get("/results/{job_id}")
def get_results(job_id: str):
    if job_id not in job_results:
        raise HTTPException(status_code=404, detail="Job ID not found or still processing")
    return {"job_id": job_id, "results": job_results[job_id]}
