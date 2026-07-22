from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import time, uuid, base64

app = FastAPI(title="LogIQ Service")

jobs = {}  # in-memory store — fine for a POC (resets when the service restarts)

class LogPayload(BaseModel):
    fileName: str
    content: str  # base64-encoded file contents

def analyze(job_id: str, raw_b64: str):
    text = base64.b64decode(raw_b64).decode("utf-8", errors="ignore")
    lines = text.splitlines()

    errors   = sum(1 for l in lines if "error" in l.lower() or "exception" in l.lower())
    warnings = sum(1 for l in lines if "warn"  in l.lower())

    # animate progress over ~3s so the UI has something to stream
    for step in range(1, 11):
        jobs[job_id].update(
            status="In Progress",
            progress=step * 10,
            message=f"Scanning log... {step * 10}%",
        )
        time.sleep(0.3)

    jobs[job_id].update(
        status="Complete",
        progress=100,
        message="Analysis complete",
        result={"totalLines": len(lines), "errors": errors, "warnings": warnings},
    )

@app.get("/")
def health():
    return {"service": "LogIQ", "status": "up"}

@app.post("/analyze")
def start(payload: LogPayload, bg: BackgroundTasks):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "Queued", "progress": 0, "message": "Queued", "result": None}
    bg.add_task(analyze, job_id, payload.content)
    return {"jobId": job_id}

@app.get("/status/{job_id}")
def status(job_id: str):
    return jobs.get(job_id, {"status": "Not Found", "progress": 0, "message": "Unknown job", "result": None})