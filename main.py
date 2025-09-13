import os
import re
import json
import base64
import time
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

# =========================
# Config
# =========================
LINK_TTL_SECONDS = int(os.getenv("LINK_TTL_SECONDS", "3600"))  # 1 hour default

# token -> {"bytes": b"...", "mime": "...", "filename": "...", "exp": unix_ts}
TOKENS: Dict[str, Dict[str, Any]] = {}

def _now() -> int:
    return int(time.time())

def _add_token(data: bytes, mime: str, filename: str) -> str:
    token = base64.urlsafe_b64encode(os.urandom(16)).decode("ascii").rstrip("=")
    TOKENS[token] = {
        "bytes": data,
        "mime": mime,
        "filename": filename,
        "exp": _now() + LINK_TTL_SECONDS,
    }
    return token

def _cleanup_tokens() -> None:
    now = _now()
    expired = [t for t, v in TOKENS.items() if v.get("exp", 0) <= now]
    for t in expired:
        TOKENS.pop(t, None)

# =========================
# App
# =========================
app = FastAPI(title="Creator Transcript Fetcher", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

class Req(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB"
    keep_timestamps: bool = False

def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr or p.stdout)
    return p.stdout

def video_id(u: str) -> str:
    """
    Extract the 11-char YouTube Video ID from watch, youtu.be, or shorts URLs.
    If it already looks like an ID, return it unchanged.
    """
    m = re.search(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})", u)
    return m.group(1) if m else u.strip()

def get_meta(url: str) -> dict:
    """
    Fetch video metadata with yt-dlp -J. If it's a playlist entry, grab the first.
    """
    out = run(["yt-dlp", "-J", "--skip-download", url])
    data = json.loads(out)
    if isinstance(data, dict) and data.get("entries"):
        data = data["entries"][0]
    return data if isinstance(data, dict) else {}

def clean_srt_to_text(srt_path: Path, keep_ts: bool) -> str:
    """
    Convert SRT text into a clean paragraph (or timestamped lines if keep_ts).
    """
    raw = srt_path.read_text(encoding="utf-8", errors="ignore")
    blocks = re.split(r"\n\s*\n", raw.strip())
    lines = []
    for blk in blocks:
        # Remove numeric index line
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)
        # Extract a timestamp if present (for keep_ts)
        m = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        # Remove the timecode line from the text
        text = re.sub(
            r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$",
            "",
            blk,
        )
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and m:
            lines.append(f"{m.group(1)} {text}")
        else:
            lines.append(text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    # De-duplicate repeated words and tidy punctuation spacing
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

def as_data_url(mime: str, content: bytes, b64: bool = False) -> str:
    if b64:
        return f"data:{mime};base64,{base64.b64encode(content).decode('ascii')}"
    else:
        from urllib.parse import quote
        return f"data:{mime},{quote(content.decode('utf-8'))}"

def _external_base_url(request: Request) -> str:
    """
    Build the external base URL behind Railway/Proxies:
    prefer X-Forwarded-Proto/Host, defaulting to https.
    """
    proto = request.headers.get("x-forwarded-proto") or "https"
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    if not host:
        # Very defensive fallback (adjust to your Railway URL if needed)
        return "https://vidalchemy-transcript-api-production.up.railway.app"
    # Force https if proto is missing or http
    if proto != "https":
        proto = "https"
    return f"{proto}://{host}"

@app.get("/file/{token}")
def file_download(token: str):
    """
    Serve a time-limited file by token with correct Content-Disposition.
    """
    _cleanup_tokens()
    item = TOKENS.get(token)
    if not item or item.get("exp", 0) < _now():
        raise HTTPException(status_code=404, detail="Link expired or not found.")
    headers = {
        "Content-Disposition": f'attachment; filename="{item["filename"]}"'
    }
    return Response(content=item["bytes"], media_type=item["mime"], headers=headers)

@app.post("/transcript")
def transcript(req: Req, request: Request):
    _cleanup_tokens()

    # Normalize URL / ID
    url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={req.url_or_id}"
    vid = video_id(url)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)

        # -------- metadata
        meta = get_meta(url)
        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        published_at = meta.get("upload_date", "")
        if published_at and len(published_at) == 8:
            published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
        duration_s = int(meta.get("duration") or 0)

        # -------- captions via yt-dlp (robust attempts)
        # Strategies:
        # 1) Manual subs for requested languages
        # 2) Auto subs for requested languages
        # 3) Auto subs for any English variant (en.*)
        # 4) Auto subs for ANY language (all)
        attempts = [
            ("--write-sub",       req.langs),
            ("--write-auto-sub",  req.langs),
            ("--write-auto-sub",  "en.*"),
            ("--write-auto-sub",  "all"),
        ]

        success = False
        last_error = None

        for mode, sublangs in attempts:
            try:
                cmd = [
                    "yt-dlp",
                    "--skip-download",
                    "--sub-langs", sublangs,
                    "--convert-subs", "srt",
                    "--force-overwrites",
                    "-o", "%(id)s.%(ext)s",
                    mode,
                    url,
                ]
                print(f"[yt-dlp] Trying {mode} with sub-langs='{sublangs}'")
                run(cmd)
                success = True
                break
            except Exception as e:
                last_error = str(e)
                print(f"[yt-dlp] Attempt failed: {mode} / '{sublangs}' -> {last_error}")

        if not success:
            raise HTTPException(status_code=404, detail="No captions available for this video.")

        # Find generated captions
        srt = next(iter(wd.glob(f"{vid}*.srt")), None)
        vtt = None if srt else next(iter(wd.glob(f"{vid}*.vtt")), None)
        if not srt and vtt:
            # Convert vtt -> srt
            run(["ffmpeg", "-y", "-i", str(vtt), str(vtt.with_suffix(".srt"))])
            srt = vtt.with_suffix(".srt")
        if not srt or not srt.exists():
            raise HTTPException(status_code=404, detail="Failed to obtain subtitles.")

        # -------- clean transcript text
        txt = clean_srt_to_text(srt, keep_ts=req.keep_timestamps)
        txt_bytes = txt.encode("utf-8")
        srt_bytes = srt.read_bytes()

        # -------- optional PDF
        pdf_bytes = None
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            header = f"{title} — {channel}\nhttps://www.youtube.com/watch?v={vid}\n\n"
            for line in (header + txt).split("\n"):
                pdf.multi_cell(0, 6, line)
            pdf_path = wd / f"transcript_{vid}.pdf"
            pdf.output(str(pdf_path))
            pdf_bytes = pdf_path.read_bytes()
        except Exception:
            pdf_bytes = None

        # -------- create tokens for HTTP downloads (force https)
        base = _external_base_url(request)
        txt_token = _add_token(txt_bytes, "text/plain; charset=utf-8", f"{vid}.txt")
        srt_token = _add_token(srt_bytes, "application/x-subrip", f"{vid}.srt")
        txt_http_url = f"{base}/file/{txt_token}"
        srt_http_url = f"{base}/file/{srt_token}"

        pdf_http_url = ""
        if pdf_bytes:
            pdf_token = _add_token(pdf_bytes, "application/pdf", f"transcript_{vid}.pdf")
            pdf_http_url = f"{base}/file/{pdf_token}"

        # Also return data URLs as fallbacks (not for primary UI)
        txt_url = as_data_url("text/plain;charset=utf-8", txt_bytes)
        srt_url = as_data_url("text/plain;charset=utf-8", srt_bytes)
        pdf_url = as_data_url("application/pdf", pdf_bytes, b64=True) if pdf_bytes else ""

        preview = txt[:2500]
        return {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "video_id": vid,
            "preview_text": preview,
            "truncated": len(txt) > len(preview),

            # preferred http links (use these in GPT UI)
            "txt_http_url": txt_http_url,
            "srt_http_url": srt_http_url,
            "pdf_http_url": pdf_http_url,
            "links_expire_in_seconds": LINK_TTL_SECONDS,

            # fallback data urls (avoid in UI, but useful for debugging)
            "txt_url": txt_url,
            "srt_url": srt_url,
            "pdf_url": pdf_url,
        }
