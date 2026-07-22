from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import uuid, base64

app = FastAPI(title="LogIQ Service")

jobs = {}  # in-memory store — fine for a POC (resets when the service restarts)

class LogPayload(BaseModel):
    fileName: str
    content: str  # base64-encoded file contents

# ordered pipeline stages, each mapped to the progress value it starts at
STAGES = [
    ("Received",   0),
    ("Decoding",   10),
    ("Parsing",    25),
    ("Analyzing",  40),   # the real work happens across 40–90
    ("Finalizing", 90),
    ("Complete",   100),
]

def set_stage(job_id, stage_name, progress=None, message=None):
    base = dict(jobs[job_id])
    base["stage"] = stage_name
    if progress is not None:
        base["progress"] = progress
    if message is not None:
        base["message"] = message
    base["status"] = "Complete" if stage_name == "Complete" else "In Progress"
    jobs[job_id] = base

def analyze(job_id: str, raw_b64: str):
    set_stage(job_id, "Received", 0, "File received")

    set_stage(job_id, "Decoding", 10, "Decoding file")
    text = base64.b64decode(raw_b64).decode("utf-8", errors="ignore")

    set_stage(job_id, "Parsing", 25, "Splitting into lines")
    lines = text.splitlines()
    total = len(lines)

    set_stage(job_id, "Analyzing", 40, f"Analyzing {total} lines")
    errors = 0
    warnings = 0
    step = max(1, total // 100)
    for i, line in enumerate(lines):
        low = line.lower()
        if "error" in low or "exception" in low:
            errors += 1
        if "warn" in low:
            warnings += 1
        if i % step == 0 and total > 0:
            # spread real progress across the 40–90 band reserved for analysis
            pct = 40 + int((i / total) * 50)
            jobs[job_id].update(progress=pct, message=f"Processed {i} of {total} lines")

    set_stage(job_id, "Finalizing", 90, "Building result")

    result = {"totalLines": total, "errors": errors, "warnings": warnings}
    set_stage(job_id, "Complete", 100, "Analysis complete")
    jobs[job_id]["result"] = result

@app.get("/")
def health():
    return {"service": "LogIQ", "status": "up"}

@app.post("/analyze")
def start(payload: LogPayload, bg: BackgroundTasks):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "Queued", "stage": "Queued", "progress": 0,
                    "message": "Queued", "result": None}
    bg.add_task(analyze, job_id, payload.content)
    return {"jobId": job_id}

@app.get("/status/{job_id}")
def status(job_id: str):
    return jobs.get(job_id, {"status": "Not Found", "stage": "Not Found",
                             "progress": 0, "message": "Unknown job", "result": None})