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
from urllib.parse import urljoin

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import Response

# =========================
# Config & Flags
# =========================
APP_TITLE = "Creator Transcript Fetcher"
APP_VERSION = "1.3.0"

# Expiring in-memory files (tokens) — default 24h
FILE_TTL_SECONDS = int(os.getenv("FILE_TTL_SECONDS", "86400"))

# Absolute base for download links (required for ChatGPT)
# e.g. https://vidalchemy-transcript-api-production.up.railway.app
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

# Include debug block in JSON when true
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

DEFAULT_LANGS = "en,en-US,en-GB"

# =========================
# In-memory file store
# =========================
# token -> {content, mime, filename, exp}
_FILE_STORE: Dict[str, Dict[str, Any]] = {}

def _now() -> float:
    return time.time()

def _store_file(content: bytes, mime: str, filename: str) -> str:
    token = base64.urlsafe_b64encode(os.urandom(24)).decode("ascii").rstrip("=")
    _FILE_STORE[token] = {
        "content": content,
        "mime": mime,
        "filename": filename,
        "exp": _now() + FILE_TTL_SECONDS,
    }
    return token

def _cleanup_files() -> None:
    now = _now()
    expired = [t for t, v in _FILE_STORE.items() if v["exp"] < now]
    for t in expired:
        _FILE_STORE.pop(t, None)

def _abs_url(path: str) -> str:
    """Return absolute URL for ChatGPT; fall back to raw path for Swagger/local."""
    if not PUBLIC_BASE_URL:
        return path
    return urljoin(PUBLIC_BASE_URL + "/", path.lstrip("/"))

# =========================
# FastAPI
# =========================
app = FastAPI(title=APP_TITLE, version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False
)

# =========================
# Models
# =========================
class Req(BaseModel):
    url_or_id: str
    langs: str = DEFAULT_LANGS
    keep_timestamps: bool = False

# =========================
# Utilities
# =========================
ID_RE = re.compile(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})")

def extract_video_id(u: str) -> str:
    u = u.strip()
    m = ID_RE.search(u)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", u):
        return u
    return u

def run(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr

def get_meta_with_ytdlp(url: str, workdir: Path, dbg: List[str]) -> Dict[str, Any]:
    cmd = ["yt-dlp", "-J", "--skip-download", url]
    rc, out, err = run(cmd, cwd=workdir)
    if DEBUG:
        dbg.append(f"[meta] rc={rc}\nCMD: {' '.join(cmd)}\nSTDERR:\n{err[:1000]}")
    if rc != 0:
        return {}
    try:
        data = json.loads(out)
        if isinstance(data, dict) and data.get("entries"):
            data = data["entries"][0]
        return data if isinstance(data, dict) else {}
    except Exception as e:
        if DEBUG:
            dbg.append(f"[meta] JSON parse error: {e}")
        return {}

def clean_srt_to_text(srt_text: str, keep_ts: bool) -> str:
    blocks = re.split(r"\n\s*\n", srt_text.strip(), flags=re.MULTILINE)
    lines: List[str] = []
    ts_re = re.compile(r"^\s*\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$", re.M)
    for blk in blocks:
        blk2 = re.sub(r"^\s*\d+\s*\r?\n", "", blk)
        ts_match = ts_re.search(blk2)
        text = ts_re.sub("", blk2)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and ts_match:
            start_ts = ts_match.group(0).split("-->")[0].strip().split(",")[0]
            lines.append(f"{start_ts} {text}")
        else:
            lines.append(text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

def direct_timedtext_probe(vid: str, langs_csv: str, dbg: List[str]) -> Dict[str, Any]:
    lang = (langs_csv.split(",")[0] or "en").strip()
    url = f"https://www.youtube.com/api/timedtext?lang={lang}&v={vid}"
    try:
        r = requests.get(url, timeout=10)
        return {"url": url, "status": r.status_code, "len": len(r.text)}
    except Exception as e:
        if DEBUG:
            dbg.append(f"[timedtext] probe error: {e}")
        return {"url": url, "status": -1, "len": 0}

def try_subs(url: str, workdir: Path, langs: str, dbg: List[str]) -> Tuple[Optional[Path], Dict[str, Any]]:
    attempt_log: Dict[str, Any] = {"attempts": []}

    def _run_attempt(label: str, write_flag: str, sub_lang: str) -> Optional[Path]:
        cmd = [
            "yt-dlp", "--skip-download", write_flag,
            "--sub-langs", sub_lang, "--convert-subs", "srt",
            "--force-overwrites", "-o", "%(id)s.%(ext)s", url,
        ]
        rc, out, err = run(cmd, cwd=workdir)
        attempt_log["attempts"].append(
            {"label": label, "rc": rc, "cmd": " ".join(cmd), "stderr": err[:1000]}
        )
        if rc == 0:
            srt_list = list(workdir.glob("*.srt"))
            if srt_list:
                srt_list.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                return srt_list[0]
        return None

    p = _run_attempt("manual/requested", "--write-sub", langs)
    if p: return p, attempt_log
    p = _run_attempt("auto/requested", "--write-auto-sub", langs)
    if p: return p, attempt_log
    p = _run_attempt("auto/wildcard", "--write-auto-sub", "en,*en*")
    if p: return p, attempt_log

    return None, attempt_log

def _pdf_sanitize(s: str) -> str:
    normalized = unicodedata.normalize("NFKD", s)
    return normalized.encode("latin-1", "ignore").decode("latin-1")

def fmt_hms(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def fmt_pretty_duration(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s and not h: parts.append(f"{s}s")
    return " ".join(parts) or "0s"

def humanize_seconds(sec: int) -> str:
    sec = max(0, int(sec or 0))
    h = sec // 3600
    m = (sec % 3600) // 60
    parts = []
    if h: parts.append(f"{h}h")
    if m or not parts: parts.append(f"{m}m")
    return " ".join(parts)

# =========================
# Routes
# =========================
@app.get("/health")
def health():
    _cleanup_files()
    return {
        "ok": True,
        "time": _now(),
        "ttl_seconds": FILE_TTL_SECONDS,
        "base": PUBLIC_BASE_URL or "(relative links)"
    }

@app.get("/file/{token}")
def get_file(token: str):
    _cleanup_files()
    item = _FILE_STORE.get(token)
    if not item or item["exp"] < _now():
        raise HTTPException(status_code=404, detail="File expired or not found.")
    headers = {
        "Content-Type": item["mime"],
        "Content-Disposition": f'attachment; filename="{item["filename"]}"',
        "Cache-Control": "no-store",
    }
    return Response(content=item["content"], media_type=item["mime"], headers=headers)

@app.post("/transcript")
def transcript(req: Req):
    dbg: List[str] = []
    _cleanup_files()

    # Normalize URL/ID and probe
    vid = extract_video_id(req.url_or_id)
    if DEBUG:
        dbg.append(f"[extract] input='{req.url_or_id}' -> video_id='{vid}'")
    tt = direct_timedtext_probe(vid, req.langs, dbg)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)

        # Metadata
        meta = get_meta_with_ytdlp(req.url_or_id, wd, dbg)
        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        published_at = meta.get("upload_date", "")
        if published_at and len(published_at) == 8:
            published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
        duration_s = int(meta.get("duration") or 0)
        meta_vid = meta.get("id") or vid

        # Captions
        srt_path, attempts = try_subs(req.url_or_id, wd, req.langs, dbg)
        if not srt_path or not srt_path.exists():
            detail = {
                "message": "Failed to obtain subtitles.",
                "video_id_extracted": vid,
                "video_id_metadata": meta_vid,
                "timedtext_probe": tt,
                "ytdlp_attempts": attempts["attempts"],
            }
            raise HTTPException(status_code=404, detail=detail)

        # Build TXT
        srt_bytes = srt_path.read_bytes()
        srt_text = srt_bytes.decode("utf-8", errors="ignore")
        txt_text = clean_srt_to_text(srt_text, keep_ts=req.keep_timestamps)
        txt_bytes = txt_text.encode("utf-8")

        # Optional PDF
        pdf_bytes = b""
        pdf_error = ""
        pdf_http_url = ""
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Helvetica", size=12)

            header = f"{title} — {channel}\nhttps://www.youtube.com/watch?v={meta_vid}\n\n"
            safe_text = _pdf_sanitize(header + txt_text)
            for paragraph in safe_text.split("\n"):
                pdf.multi_cell(0, 6, paragraph)

            pdf_path = wd / f"transcript_{meta_vid}.pdf"
            pdf.output(str(pdf_path))
            pdf_bytes = pdf_path.read_bytes()
        except Exception as e:
            pdf_error = f"{type(e).__name__}: {e}"

        # Tokens & absolute URLs
        primary_lang = (req.langs.split(",")[0] or "en").strip().replace("*", "en")
        txt_filename = f"{meta_vid}_{primary_lang}.txt"
        srt_filename = f"{meta_vid}_{primary_lang}.srt"
        pdf_filename = f"transcript_{meta_vid}.pdf"

        txt_token = _store_file(txt_bytes, "text/plain; charset=utf-8", txt_filename)
        srt_token = _store_file(srt_bytes, "text/plain; charset=utf-8", srt_filename)
        if pdf_bytes:
            pdf_token = _store_file(pdf_bytes, "application/pdf", pdf_filename)
            pdf_http_url = _abs_url(f"/file/{pdf_token}")

        # Response
        result = {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "duration_hms": fmt_hms(duration_s),
            "duration_pretty": fmt_pretty_duration(duration_s),
            "video_id": meta_vid,
            "preview_text": txt_text[:2500],
            "truncated": len(txt_text) > 2500,
            "txt_http_url": _abs_url(f"/file/{txt_token}"),
            "srt_http_url": _abs_url(f"/file/{srt_token}"),
            "pdf_http_url": pdf_http_url,
            "links_expire_in_seconds": FILE_TTL_SECONDS,
            "links_expire_human": humanize_seconds(FILE_TTL_SECONDS),
        }
        if DEBUG:
            result["debug"] = {
                "extracted_video_id": vid,
                "timedtext_probe": tt,
                "ytdlp_attempts": attempts["attempts"],
                "meta_id": meta_vid,
                "pdf_error": pdf_error,
            }
        return result
