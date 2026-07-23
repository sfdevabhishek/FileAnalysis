from fastapi import FastAPI, BackgroundTasks, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import uuid, base64, asyncio

app = FastAPI(title="LogIQ Service")

jobs = {}     # job_id -> latest state (also serves the /status fallback)
payloads = {} # job_id -> base64 content, waiting for the socket to connect

class LogPayload(BaseModel):
    fileName: str
    content: str  # base64-encoded file contents

STAGES = [
    ("Received",   0),
    ("Decoding",   10),
    ("Parsing",    25),
    ("Analyzing",  40),
    ("Finalizing", 90),
    ("Complete",   100),
]

async def analyze(job_id: str, raw_b64: str, ws: WebSocket):
    async def push(stage, progress, message, status="In Progress", result=None):
        update = {"jobId": job_id, "stage": stage, "progress": progress,
                  "message": message, "status": status, "result": result}
        jobs[job_id] = update
        await ws.send_json(update)

    await push("Received", 0, "File received")
    await asyncio.sleep(0.2)  # small pauses so the stages are visible in the UI

    await push("Decoding", 10, "Decoding file")
    text = base64.b64decode(raw_b64).decode("utf-8", errors="ignore")
    await asyncio.sleep(0.2)

    await push("Parsing", 25, "Splitting into lines")
    lines = text.splitlines()
    total = len(lines)
    await asyncio.sleep(0.2)

    await push("Analyzing", 40, f"Analyzing {total} lines")
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
            pct = 40 + int((i / total) * 50)   # real progress in the 40–90 band
            update = {"jobId": job_id, "stage": "Analyzing", "progress": pct,
                      "message": f"Processed {i} of {total} lines",
                      "status": "In Progress", "result": None}
            jobs[job_id] = update
            await ws.send_json(update)
            await asyncio.sleep(0)  # yield so sends actually flush

    await push("Finalizing", 90, "Building result")
    await asyncio.sleep(0.2)

    result = {"totalLines": total, "errors": errors, "warnings": warnings}
    await push("Complete", 100, "Analysis complete", status="Complete", result=result)

@app.get("/")
def health():
    return {"service": "LogIQ", "status": "up"}

@app.post("/analyze")
def start(payload: LogPayload):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"jobId": job_id, "status": "Queued", "stage": "Queued",
                    "progress": 0, "message": "Waiting for connection", "result": None}
    payloads[job_id] = payload.content   # held until the socket connects
    return {"jobId": job_id}

@app.websocket("/ws/{job_id}")
async def ws_progress(websocket: WebSocket, job_id: str):
    await websocket.accept()
    raw = payloads.pop(job_id, None)
    if raw is None:
        await websocket.send_json({"jobId": job_id, "status": "Error", "stage": "Error",
                                   "progress": 0, "message": "Unknown or already-run job",
                                   "result": None})
        await websocket.close()
        return
    try:
        await analyze(job_id, raw, websocket)
    except WebSocketDisconnect:
        pass
    finally:
        await websocket.close()

@app.get("/status/{job_id}")
def status(job_id: str):
    return jobs.get(job_id, {"jobId": job_id, "status": "Not Found", "stage": "Not Found",
                             "progress": 0, "message": "Unknown job", "result": None})