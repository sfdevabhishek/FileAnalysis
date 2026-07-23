from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import uuid, base64, asyncio, os, hashlib, logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("logiq")

app = FastAPI(title="LogIQ Service")

jobs = {}     # job_id -> latest state (also serves /status fallback)
payloads = {} # job_id -> base64 content (used by the HTTP-upload path)

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

# ---------- shared analysis, reading from a file already on disk ----------
async def analyze_from_disk(job_id: str, path: str, ws: WebSocket):
    async def push(stage, progress, message, status="In Progress", result=None):
        update = {"jobId": job_id, "stage": stage, "progress": progress,
                  "message": message, "status": status, "result": result}
        jobs[job_id] = update
        await ws.send_json(update)

    size = os.path.getsize(path)

    # checksum the whole file (proves full-file integrity)
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    checksum = h.hexdigest()[:12]
    log.info(f"job={job_id} on disk {size} bytes checksum={checksum}")

    await push("Parsing", 25, "Preparing to read file")

    await push("Analyzing", 40, f"Reading file ({size} bytes)")
    errors = 0
    warnings = 0
    total = 0
    read = 0
    last_pct = 40
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            read += len(line.encode("utf-8", errors="ignore"))
            total += 1
            low = line.lower()
            if "error" in low or "exception" in low:
                errors += 1
            if "warn" in low:
                warnings += 1
            pct = 40 + int((read / size) * 50) if size else 90
            if pct != last_pct:
                last_pct = pct
                update = {"jobId": job_id, "stage": "Analyzing", "progress": pct,
                          "message": f"Read {read} of {size} bytes ({total} lines)",
                          "status": "In Progress", "result": None}
                jobs[job_id] = update
                await ws.send_json(update)
                log.info(f"job={job_id} bytes={read}/{size} lines={total} progress={pct}")
                await asyncio.sleep(0)

    await push("Finalizing", 90, "Building result")

    result = {"totalLines": total, "errors": errors, "warnings": warnings,
              "checksum": checksum, "bytes": size}
    log.info(f"job={job_id} COMPLETE lines={total} errors={errors} "
             f"warnings={warnings} checksum={checksum}")

    try:
        os.remove(path)
    except OSError:
        pass

    await push("Complete", 100, "Analysis complete", status="Complete", result=result)

# ---------- health ----------
@app.get("/")
def health():
    return {"service": "LogIQ", "status": "up"}

# ---------- OLD HTTP upload path (kept so you can compare) ----------
@app.post("/analyze")
def start(payload: LogPayload):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"jobId": job_id, "status": "Queued", "stage": "Queued",
                    "progress": 0, "message": "Waiting for connection", "result": None}
    payloads[job_id] = payload.content
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
    # HTTP path: decode base64 to disk, then reuse analyze_from_disk
    path = f"/tmp/{job_id}.log"
    with open(path, "wb") as f:
        f.write(base64.b64decode(raw))
    try:
        await analyze_from_disk(job_id, path, websocket)
    except WebSocketDisconnect:
        pass
    finally:
        await websocket.close()

# ---------- NEW: file ingestion over WebSocket (the learning exercise) ----------
@app.websocket("/ws/upload/{job_id}")
async def ws_upload(websocket: WebSocket, job_id: str):
    await websocket.accept()
    path = f"/tmp/{job_id}.log"
    expected = None
    received = 0

    jobs[job_id] = {"jobId": job_id, "status": "Uploading", "stage": "Uploading",
                    "progress": 0, "message": "Receiving chunks", "result": None}

    try:
        with open(path, "wb") as f:
            while True:
                msg = await websocket.receive_json()
                t = msg.get("type")

                if t == "meta":
                    expected = msg["totalChunks"]
                    log.info(f"job={job_id} upload starting, {expected} chunks")
                    await websocket.send_json({"type": "ready"})

                elif t == "chunk":
                    # NAIVE: trusts order, no ack, no validation of index
                    f.write(base64.b64decode(msg["data"]))
                    received += 1
                    pct = int((received / expected) * 100) if expected else 0
                    await websocket.send_json({"type": "upload_progress", "progress": pct})

                elif t == "done":
                    log.info(f"job={job_id} upload done, {received}/{expected} chunks")
                    break

        # reassembled — analyze over the SAME socket
        await analyze_from_disk(job_id, path, websocket)

    except WebSocketDisconnect:
        log.info(f"job={job_id} client disconnected mid-upload — partial file left behind")
    finally:
        await websocket.close()

# ---------- status fallback ----------
@app.get("/status/{job_id}")
def status(job_id: str):
    return jobs.get(job_id, {"jobId": job_id, "status": "Not Found", "stage": "Not Found",
                             "progress": 0, "message": "Unknown job", "result": None})