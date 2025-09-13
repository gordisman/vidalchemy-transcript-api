import os
import re
import json
import base64
import tempfile
import subprocess
import time
import unicodedata
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import Response

# ------------------------------
# In-memory file store (expiring)
# ------------------------------
# CHANGED: default TTL -> 24h, and allow env override
FILE_TTL_SECONDS = int(os.getenv("FILE_TTL_SECONDS", "86400"))  # 24 hours
_file_store: Dict[str, Dict[str, Any]] = {}  # token -> {content, mime, exp, filename}

def _store_file(content: bytes, mime: str, filename: str) -> str:
    token = base64.urlsafe_b64encode(os.urandom(24)).decode("ascii").rstrip("=")
    _file_store[token] = {
        "content": content,
        "mime": mime,
        "exp": time.time() + FILE_TTL_SECONDS,
        "filename": filename,
    }
    return token

def _cleanup_files():
    now = time.time()
    expired = [t for t, v in _file_store.items() if v["exp"] < now]
    for t in expired:
        _file_store.pop(t, None)

# -------------
# FastAPI setup
# -------------
app = FastAPI(title="Creator Transcript Fetcher (DEBUG)", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

# ------------
# Models
# ------------
class Req(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB"
    keep_timestamps: bool = False

# ------------
# Helpers
# ------------
ID_RE = re.compile(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})")

def extract_video_id(u: str) -> str:
    """
    Robust extractor: accept watch, youtu.be, shorts, or a raw 11-char ID.
    """
    u = u.strip()
    m = ID_RE.search(u)
    if m:
        return m.group(1)
    # If user already gave an 11-char ID, accept it.
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", u):
        return u
    return u  # As last resort; yt-dlp will still try the full URL

def run(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    """
    Run a command and capture returncode, stdout, stderr (as text).
    """
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
    )
    return p.returncode, p.stdout, p.stderr

def get_meta_with_ytdlp(url: str, workdir: Path, debug: List[str]) -> Dict[str, Any]:
    """
    Use yt-dlp -J to fetch metadata. Return a dict or {} on failure.
    """
    cmd = ["yt-dlp", "-J", "--skip-download", url]
    rc, out, err = run(cmd, cwd=workdir)
    debug.append(f"[meta] rc={rc}\nCMD: {' '.join(cmd)}\nSTDERR:\n{err[:2000]}")
    if rc != 0:
        return {}
    try:
        data = json.loads(out)
        if isinstance(data, dict) and data.get("entries"):
            data = data["entries"][0]
        return data if isinstance(data, dict) else {}
    except Exception as e:
        debug.append(f"[meta] JSON parse error: {e}")
        return {}

def clean_srt_to_text(srt_text: str, keep_ts: bool) -> str:
    """
    Simple SRT cleaner: removes index/timestamp lines.
    Keeps timestamps if keep_ts=True (first timestamp per block).
    """
    blocks = re.split(r"\n\s*\n", srt_text.strip(), flags=re.MULTILINE)
    lines: List[str] = []
    ts_re = re.compile(r"^\s*\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$", re.M)

    for blk in blocks:
        blk2 = re.sub(r"^\s*\d+\s*\r?\n", "", blk)  # drop index
        ts_match = ts_re.search(blk2)
        text = ts_re.sub("", blk2)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and ts_match:
            start_ts = ts_match.group(0).split("-->")[0].strip().split(",")[0]  # HH:MM:SS
            lines.append(f"{start_ts} {text}")
        else:
            lines.append(text)

    return ("\n".join(lines)) if keep_ts else (" ".join(lines))

def direct_timedtext_probe(vid: str, langs_csv: str, debug: List[str]) -> Dict[str, Any]:
    """
    Probe YouTube timedtext endpoint directly for the first language in langs.
    Returns status, length, and url tested—purely for debugging visibility.
    """
    lang = (langs_csv.split(",")[0] or "en").strip()
    url = f"https://www.youtube.com/api/timedtext?lang={lang}&v={vid}"
    try:
        r = requests.get(url, timeout=10)
        return {"url": url, "status": r.status_code, "len": len(r.text)}
    except Exception as e:
        debug.append(f"[timedtext] probe error: {e}")
        return {"url": url, "status": -1, "len": 0}

def try_subs(url: str, workdir: Path, langs: str, debug: List[str]) -> Tuple[Optional[Path], Dict[str, Any]]:
    """
    Attempt to download subtitles with yt-dlp:
      1) manual subs, given langs
      2) auto subs, given langs
      3) auto subs, wildcard 'en,*' (last resort)
    Returns (srt_path, attempt_log_dict).
    """
    attempt_log: Dict[str, Any] = {"attempts": []}

    def _run_attempt(label: str, write_flag: str, sub_lang: str) -> Optional[Path]:
        cmd = [
            "yt-dlp",
            "--skip-download",
            write_flag,
            "--sub-langs", sub_lang,
            "--convert-subs", "srt",
            "--force-overwrites",
            "-o", "%(id)s.%(ext)s",
            url,
        ]
        rc, out, err = run(cmd, cwd=workdir)
        attempt_log["attempts"].append(
            {"label": label, "rc": rc, "cmd": " ".join(cmd), "stderr": err[:2000]}
        )
        if rc == 0:
            srt_list = list(workdir.glob("*.srt"))
            if srt_list:
                srt_list.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                return srt_list[0]
        return None

    p = _run_attempt("manual/requested", "--write-sub", langs)
    if p:
        return p, attempt_log

    p = _run_attempt("auto/requested", "--write-auto-sub", langs)
    if p:
        return p, attempt_log

    p = _run_attempt("auto/wildcard", "--write-auto-sub", "en,*en*")
    if p:
        return p, attempt_log

    return None, attempt_log

# NEW: make text safe for core PDF fonts (latin-1)
def _pdf_sanitize(s: str) -> str:
    # Replace non-latin-1 chars with a close ASCII approximation (or drop).
    normalized = unicodedata.normalize("NFKD", s)
    return normalized.encode("latin-1", "ignore").decode("latin-1")

# --------------
# Routes
# --------------
@app.get("/health")
def health():
    _cleanup_files()
    return {"ok": True, "time": time.time(), "ttl_seconds": FILE_TTL_SECONDS}

@app.get("/file/{token}")
def get_file(token: str):
    _cleanup_files()
    item = _file_store.get(token)
    if not item or item["exp"] < time.time():
        raise HTTPException(status_code=404, detail="File expired or not found.")
    headers = {
        "Content-Type": item["mime"],
        "Content-Disposition": f'attachment; filename="{item["filename"]}"',
        "Cache-Control": "no-store",
    }
    return Response(content=item["content"], media_type=item["mime"], headers=headers)

@app.post("/transcript")
def transcript(req: Req):
    debug: List[str] = []
    _cleanup_files()

    # Extract a best-effort VideoID for visibility
    vid = extract_video_id(req.url_or_id)
    debug.append(f"[extract] input='{req.url_or_id}' -> video_id='{vid}'")

    # Timedtext probe (pure visibility into public availability)
    tt = direct_timedtext_probe(vid, req.langs, debug)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)

        # Fetch metadata
        meta = get_meta_with_ytdlp(req.url_or_id, wd, debug)
        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        published_at = meta.get("upload_date", "")
        if published_at and len(published_at) == 8:
            published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
        duration_s = int(meta.get("duration") or 0)
        meta_vid = meta.get("id") or vid

        # Try to obtain SRT
        srt_path, attempts = try_subs(req.url_or_id, wd, req.langs, debug)

        if not srt_path or not srt_path.exists():
            detail = {
                "message": "Failed to obtain subtitles.",
                "video_id_extracted": vid,
                "video_id_metadata": meta_vid,
                "timedtext_probe": tt,
                "ytdlp_attempts": attempts["attempts"],
            }
            raise HTTPException(status_code=404, detail=detail)

        # Read SRT & create TXT
        srt_bytes = srt_path.read_bytes()
        srt_text = srt_bytes.decode("utf-8", errors="ignore")
        txt_text = clean_srt_to_text(srt_text, keep_ts=req.keep_timestamps)
        txt_bytes = txt_text.encode("utf-8")

        # Robust PDF creation (no trimming — paginate & wrap)
        pdf_bytes = b""
        pdf_http_url = ""
        pdf_error = ""
        try:
            from fpdf import FPDF  # core fonts only (latin-1)
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Helvetica", size=12)  # CHANGED: core font always available

            header = f"{title} — {channel}\nhttps://www.youtube.com/watch?v={meta_vid}\n\n"
            # Sanitize for latin-1; multi_cell will wrap and paginate automatically.
            safe_text = _pdf_sanitize(header + txt_text)

            # Write in safe chunks to avoid any weird extremely long line edge cases
            # (this does NOT trim content—just writes sequentially).
            for paragraph in safe_text.split("\n"):
                pdf.multi_cell(0, 6, paragraph)

            pdf_path = wd / f"transcript_{meta_vid}.pdf"
            pdf.output(str(pdf_path))
            pdf_bytes = pdf_path.read_bytes()
        except Exception as e:
            pdf_error = f"{type(e).__name__}: {e}"
            debug.append(f"[pdf] generation failed: {pdf_error}")

        # Create download tokens (with filenames)
        # Pick a primary language tag for the filename
        primary_lang = (req.langs.split(",")[0] or "en").strip().replace("*", "en")
        txt_filename = f"{meta_vid}_{primary_lang}.txt"
        srt_filename = f"{meta_vid}_{primary_lang}.srt"
        pdf_filename = f"transcript_{meta_vid}.pdf"

        txt_token = _store_file(txt_bytes, "text/plain; charset=utf-8", txt_filename)
        srt_token = _store_file(srt_bytes, "text/plain; charset=utf-8", srt_filename)
        if pdf_bytes:
            pdf_token = _store_file(pdf_bytes, "application/pdf", pdf_filename)
            pdf_http_url = f"/file/{pdf_token}"

        result = {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "video_id": meta_vid,
            "preview_text": txt_text[:2500],
            "truncated": len(txt_text) > 2500,
            "txt_http_url": f"/file/{txt_token}",
            "srt_http_url": f"/file/{srt_token}",
            "pdf_http_url": pdf_http_url,
            "links_expire_in_seconds": FILE_TTL_SECONDS,
            # -- DEBUG INFO --
            "debug": {
                "extracted_video_id": vid,
                "timedtext_probe": tt,             # status + length of XML
                "ytdlp_attempts": attempts["attempts"],  # every command + rc + stderr
                "meta_id": meta_vid,
                "pdf_error": pdf_error,            # NEW: see why PDF would fail (if it does)
            },
        }
        return result
