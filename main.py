import os
import re
import io
import json
import time
import uuid
import base64
import shutil
import string
import random
import logging
import tempfile
import threading
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse
from pydantic import BaseModel

# ------------------------------
# Config (via env, with defaults)
# ------------------------------
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
FILE_TTL_SECONDS = int(os.getenv("FILE_TTL_SECONDS", "86400"))  # 24h default
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "2"))               # parallel jobs
PREVIEW_CHARS = int(os.getenv("PREVIEW_CHARS", "3000"))        # preview size
KEEP_TS_DEFAULT = os.getenv("KEEP_TS_DEFAULT", "false").lower() == "true"

# -------------
# App + CORS
# -------------
app = FastAPI(title="Creator Transcript Fetcher (job+poll)", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False
)

# -----------------
# In-memory stores
# -----------------
file_lock = threading.Lock()
files: dict[str, dict] = {}  # token -> {"bytes":b,"mime":str,"filename":str,"expires":float}

job_lock = threading.Lock()
jobs: dict[str, dict] = {}   # job_id -> {"status":str, "result":dict|None, "error":str|None, "created":float}

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
logger = logging.getLogger("uvicorn.error")

# -----------------
# Utility functions
# -----------------
def run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip())
    return p.stdout

def video_id(u: str) -> str:
    m = re.search(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})", u)
    return m.group(1) if m else u.strip()

def get_meta(url: str) -> dict:
    out = run(["yt-dlp", "-J", "--skip-download", url])
    data = json.loads(out)
    if isinstance(data, dict) and data.get("entries"):
        data = data["entries"][0]
    return data if isinstance(data, dict) else {}

def hhmmss(seconds: int) -> str:
    if not seconds:
        return "0s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def clean_srt_to_text(srt_path: Path, keep_ts: bool) -> str:
    raw = srt_path.read_text(encoding="utf-8", errors="ignore")
    blocks = re.split(r"\n\s*\n", raw.strip())
    lines = []
    for blk in blocks:
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)  # remove index
        m = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        text = re.sub(r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$", "", blk)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and m:
            lines.append(f"{m.group(1)} {text}")
        else:
            lines.append(text)
    if keep_ts:
        out = "\n".join(lines)
    else:
        out = " ".join(lines)
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)  # de-dupe
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

def register_file(contents: bytes, mime: str, filename: str) -> dict:
    token = uuid.uuid4().hex
    expires = time.time() + FILE_TTL_SECONDS
    with file_lock:
        files[token] = {"bytes": contents, "mime": mime, "filename": filename, "expires": expires}
    http_url = f"{PUBLIC_BASE_URL}/file/{token}" if PUBLIC_BASE_URL else f"/file/{token}"
    return {"token": token, "http_url": http_url, "expires_in_seconds": FILE_TTL_SECONDS}

def make_pdf_bytes(title: str, channel: str, url: str, txt: str) -> bytes | None:
    try:
        from fpdf import FPDF  # heavyweight import, keep local
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        header = f"{title} — {channel}\n{url}\n\n"
        for line in (header + txt).split("\n"):
            pdf.multi_cell(0, 6, line)
        buf = io.BytesIO()
        pdf.output(buf)
        return buf.getvalue()
    except Exception as e:
        logger.warning(f"PDF generation skipped: {e}")
        return None

def try_fetch_subs(url: str, langs: str, tmp: Path) -> tuple[Path, str]:
    """
    Attempt subtitles with a strict language list first (langs as provided),
    then a permissive fallback '*,' + langs to accept ANY language if strict fails.
    Returns (srt_path, lang_used).
    """
    # common base args
    out_tmpl = "%(id)s.%(ext)s"
    base = [
        "yt-dlp", "--skip-download",
        "--convert-subs", "srt",
        "--force-overwrites",
        "-o", out_tmpl,
        url,
    ]

    # attempt order
    attempts = [
        ("strict", langs),
        ("fallback_any", f"*,{langs}" if langs else "*"),
    ]

    for mode, sub_langs in attempts:
        for write_flag in ("--write-sub", "--write-auto-sub"):
            try:
                cmd = base[:1] + [write_flag] + ["--sub-langs", sub_langs] + base[1:]
                run(cmd)
                # find any produced .srt for this video id
                for p in tmp.glob("*.srt"):
                    return p, sub_langs
            except Exception:
                continue

    raise HTTPException(status_code=404, detail="No captions were found for this video (manual+auto).")

# -----------------------
# Background janitor
# -----------------------
def janitor_loop():
    while True:
        now = time.time()
        with file_lock:
            dead = [t for t, meta in files.items() if meta["expires"] < now]
            for t in dead:
                files.pop(t, None)
        # Clean old jobs (7 days)
        with job_lock:
            dead_jobs = [j for j, d in jobs.items() if now - d.get("created", now) > 7 * 86400]
            for j in dead_jobs:
                jobs.pop(j, None)
        time.sleep(60)

threading.Thread(target=janitor_loop, daemon=True).start()

# -----------------------
# Models
# -----------------------
class StartReq(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB"
    keep_timestamps: bool = KEEP_TS_DEFAULT

# -----------------------
# Job worker
# -----------------------
def process_job(job_id: str, req: StartReq):
    try:
        url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={req.url_or_id}"
        vid = video_id(url)

        with tempfile.TemporaryDirectory() as td:
            wd = Path(td)

            # Metadata
            meta = get_meta(url)
            title = meta.get("title", "")
            channel = meta.get("channel") or meta.get("uploader", "")
            published_at = meta.get("upload_date", "")
            if published_at and len(published_at) == 8:
                published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
            duration_s = int(meta.get("duration") or 0)
            duration_hms = hhmmss(duration_s)

            # Subtitles (strict then fallback-any)
            srt_path, lang_used = try_fetch_subs(url, req.langs, wd)

            # Text
            txt = clean_srt_to_text(srt_path, keep_ts=req.keep_timestamps)
            preview = txt[:PREVIEW_CHARS]
            truncated = len(txt) > len(preview)

            # Return file links (tokenized)
            txt_bytes = txt.encode("utf-8")
            srt_bytes = srt_path.read_bytes()
            pdf_bytes = make_pdf_bytes(title, channel, url, txt)

            txt_info = register_file(txt_bytes, "text/plain; charset=utf-8", f"{vid}.txt")
            srt_info = register_file(srt_bytes, "text/plain; charset=utf-8", f"{vid}.srt")
            pdf_url = ""
            if pdf_bytes:
                pdf_info = register_file(pdf_bytes, "application/pdf", f"{vid}.pdf")
                pdf_url = pdf_info["http_url"]

            result = {
                "title": title,
                "channel": channel,
                "published_at": published_at,
                "duration_s": duration_s,
                "duration_hms": duration_hms,
                "video_id": vid,
                "preview_text": preview,
                "truncated": truncated,
                "txt_http_url": txt_info["http_url"],
                "srt_http_url": srt_info["http_url"],
                "pdf_http_url": pdf_url,
                "links_expire_in_seconds": FILE_TTL_SECONDS,
                "langs_requested": req.langs,
                "langs_used": lang_used,
            }

        with job_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["result"] = result

    except HTTPException as e:
        with job_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = e.detail
    except Exception as e:
        logger.exception("Job failed")
        with job_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)

# -----------------------
# Endpoints
# -----------------------
@app.get("/health")
def health():
    return {"ok": True, "jobs": len(jobs), "files": len(files)}

@app.get("/file/{token}")
def get_file(token: str):
    with file_lock:
        meta = files.get(token)
        if not meta:
            raise HTTPException(status_code=404, detail="File expired or not found.")
        if meta["expires"] < time.time():
            files.pop(token, None)
            raise HTTPException(status_code=404, detail="File expired.")
        data = meta["bytes"]
        mime = meta["mime"]
        filename = meta["filename"]
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=data, media_type=mime, headers=headers)

@app.post("/transcript/start")
def start(req: StartReq):
    job_id = uuid.uuid4().hex
    with job_lock:
        jobs[job_id] = {"status": "processing", "result": None, "error": None, "created": time.time()}
    executor.submit(process_job, job_id, req)
    return {"job_id": job_id, "status": "processing"}

@app.get("/transcript/result/{job_id}")
def result(job_id: str):
    with job_lock:
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Unknown job_id.")
        if job["status"] == "processing":
            return {"status": "processing"}
        if job["status"] == "error":
            return {"status": "error", "detail": job["error"]}
        return {"status": "done", "data": job["result"]}
