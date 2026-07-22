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
    total = len(lines)

    errors = 0
    warnings = 0

    jobs[job_id].update(status="In Progress", progress=0,
                        message=f"Starting analysis of {total} lines")

    for i, line in enumerate(lines):
        low = line.lower()
        if "error" in low or "exception" in low:
            errors += 1
        if "warn" in low:
            warnings += 1

        # report real progress periodically (every 1% or every 500 lines, whichever is coarser)
        step = max(1, total // 100)
        if i % step == 0 and total > 0:
            jobs[job_id].update(
                status="In Progress",
                progress=int((i / total) * 100),
                message=f"Processed {i} of {total} lines",
            )

    jobs[job_id].update(
        status="Complete",
        progress=100,
        message="Analysis complete",
        result={"totalLines": total, "errors": errors, "warnings": warnings},
    )
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