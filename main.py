from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import uuid, base64, asyncio, os, hashlib, logging
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("logiq")

app = FastAPI(title="LogIQ Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # POC only — lock this down before production
    allow_methods=["*"],
    allow_headers=["*"],
)

jobs = {}     # job_id -> latest state (also serves the /status fallback)
payloads = {} # job_id -> base64 content, held until the socket connects

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

    # --- Decode and write to disk so we process a real file ---
    await push("Decoding", 10, "Decoding file")
    text = base64.b64decode(raw_b64).decode("utf-8", errors="ignore")
    checksum = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:12]
    path = f"/tmp/{job_id}.log"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    size = os.path.getsize(path)
    log.info(f"job={job_id} wrote {size} bytes checksum={checksum}")

    await push("Parsing", 25, "Preparing to read file")

    # --- Stream-read from disk; progress = real bytes consumed ---
    await push("Analyzing", 40, f"Reading file ({size} bytes)")
    errors = 0
    warnings = 0
    total = 0
    read = 0
    last_pct = 40
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            read += len(line.encode("utf-8"))
            total += 1
            low = line.lower()
            if "error" in low or "exception" in low:
                errors += 1
            if "warn" in low:
                warnings += 1
            pct = 40 + int((read / size) * 50) if size else 90
            if pct != last_pct:  # only push when the real fraction changes
                last_pct = pct
                update = {"jobId": job_id, "stage": "Analyzing", "progress": pct,
                          "message": f"Read {read} of {size} bytes ({total} lines)",
                          "status": "In Progress", "result": None}
                jobs[job_id] = update
                await ws.send_json(update)
                log.info(f"job={job_id} bytes={read}/{size} lines={total} progress={pct}")
                await asyncio.sleep(0)  # yield so the send flushes

    await push("Finalizing", 90, "Building result")

    result = {"totalLines": total, "errors": errors, "warnings": warnings,
              "checksum": checksum, "bytes": size}
    log.info(f"job={job_id} COMPLETE lines={total} errors={errors} "
             f"warnings={warnings} checksum={checksum}")

    try:
        os.remove(path)  # clean up the temp file
    except OSError:
        pass

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