# main.py
# Creator Transcript Fetcher — stable + TimedText fallback
import os
import re
import json
import base64
import string
import random
import threading
import time
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, Tuple, Optional, List

import requests
from fastapi import FastAPI, HTTPException, Path as FPath, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

# ---------------------------
# App & CORS
# ---------------------------
app = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

# ---------------------------
# Config (via env with sane defaults)
# ---------------------------
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
LINKS_TTL_SECONDS = int(os.getenv("LINKS_TTL_SECONDS", "86400"))  # default 24h
PREVIEW_CHARS = int(os.getenv("PREVIEW_CHARS", "2800"))

# ---------------------------
# Small utilities
# ---------------------------
def run(cmd: List[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        msg = (p.stderr or p.stdout or "").strip()
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{msg}")
    return p.stdout

def pretty_duration(seconds: int) -> str:
    try:
        seconds = int(seconds)
    except Exception:
        return "0s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def video_id(u: str) -> str:
    m = re.search(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})", u)
    return m.group(1) if m else u.strip()

def get_info(url: str) -> dict:
    out = run(["yt-dlp", "-J", "--skip-download", url])
    data = json.loads(out)
    if isinstance(data, dict) and data.get("entries"):
        data = data["entries"][0]
    return data if isinstance(data, dict) else {}

def clean_srt_to_text(srt_path: Path, keep_ts: bool) -> str:
    raw = srt_path.read_text(encoding="utf-8", errors="ignore")
    blocks = re.split(r"\n\s*\n", raw.strip())
    lines = []
    for blk in blocks:
        # drop counter line
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)
        # capture timestamp line
        m = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        # remove timestamp line
        text = re.sub(r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$", "", blk)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and m:
            lines.append(f"{m.group(1)} {text}")
        else:
            lines.append(text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    # light de-dup and spacing cleanup
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

# ---------------------------
# File token store (in-memory)
# ---------------------------
class FileItem(BaseModel):
    path: str
    mime: str
    filename: str
    expires_at: float

_TOKEN_STORE: Dict[str, FileItem] = {}
_TOKEN_LOCK = threading.Lock()

def _new_token(n: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))

def register_file(path: Path, mime: str, filename: str, ttl: int) -> str:
    token = _new_token()
    item = FileItem(path=str(path), mime=mime, filename=filename, expires_at=time.time() + ttl)
    with _TOKEN_LOCK:
        _TOKEN_STORE[token] = item
        # opportunistic cleanup
        now = time.time()
        for t, it in list(_TOKEN_STORE.items()):
            if it.expires_at < now:
                _TOKEN_STORE.pop(t, None)
    return token

def token_url(request: Request, token: str) -> str:
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL}/file/{token}"
    # fallback to request base (useful in /docs)
    base = str(request.base_url).rstrip("/")
    return f"{base}/file/{token}"

# ---------------------------
# YouTube TimedText fallback
# ---------------------------
def timedtext_fallback(vid: str, wd: Path, langs_to_try: List[str]) -> Optional[Tuple[Path, str]]:
    """
    Try fetching captions directly from the YouTube TimedText endpoint.
    Returns (srt_path, lang_used) on success, else None.
    """
    def save_convert_vtt(vtt_text: str, lang: str) -> Optional[Path]:
        vtt_path = wd / f"{vid}.{lang}.vtt"
        vtt_path.write_text(vtt_text, encoding="utf-8")
        srt_path = vtt_path.with_suffix(".srt")
        try:
            run(["ffmpeg", "-y", "-i", str(vtt_path), str(srt_path)])
            if srt_path.exists():
                return srt_path
        except Exception:
            # last resort: rename VTT to SRT
            try:
                vtt_path.rename(srt_path)
                return srt_path
            except Exception:
                return None
        return None

    def tt_url(lang: str, asr: bool) -> str:
        base = f"https://www.youtube.com/api/timedtext?v={vid}&lang={lang}&fmt=vtt"
        return base + ("&kind=asr" if asr else "")

    for lang in langs_to_try:
        for asr in (False, True):
            try:
                r = requests.get(tt_url(lang, asr), timeout=12)
                if r.status_code == 200 and r.text and "WEBVTT" in r.text[:1000]:
                    srt_path = save_convert_vtt(r.text, lang)
                    if srt_path:
                        return srt_path, lang
            except Exception:
                continue
    return None

# ---------------------------
# Core subtitle fetch
# ---------------------------
def try_fetch_srt(url: str, vid: str, langs: str, wd: Path) -> Tuple[Path, str, str]:
    """
    Fetch subtitles for a single video id.

    Returns (srt_path, lang_used, source_kind)
    source_kind is one of: "manual", "auto", "timedtext"
    """
    # 1) Probe info and filter out empty tracks
    info = get_info(url)
    subs_all  = info.get("subtitles") or {}
    autos_all = info.get("automatic_captions") or {}

    def real_langs(d: dict) -> List[str]:
        real = []
        for code, formats in (d or {}).items():
            if any(isinstance(f, dict) and f.get("url") for f in (formats or [])):
                real.append(code)
        return sorted(set(real))

    manual_langs = real_langs(subs_all)
    auto_langs   = real_langs(autos_all)

    # 2) Try yt-dlp
    base = [
        "yt-dlp", "--skip-download",
        "--extractor-args", "youtube:player_client=android",
        "--convert-subs", "srt",
        "--force-overwrites",
        "-o", "%(id)s.%(ext)s",
        url,
    ]

    attempts: List[str] = []
    langs = (langs or "").strip()
    if langs:
        attempts.append(langs)            # exact user req
        attempts.append(f"*,{langs}")     # anything + user req
    attempts += ["all,-live_chat", "*", "all"]

    def find_first(*globs: str) -> Optional[Path]:
        for g in globs:
            found = sorted(wd.glob(g))
            if found:
                return found[0]
        return None

    for sub_langs in attempts:
        for flag in ("--write-sub", "--write-auto-sub"):
            try:
                cmd = base[:1] + [flag] + ["--sub-langs", sub_langs] + base[1:]
                run(cmd)
                srt = find_first(f"{vid}*.srt")
                if srt:
                    kind = "manual" if flag == "--write-sub" else "auto"
                    return srt, sub_langs, kind
                vtt = find_first(f"{vid}*.vtt")
                if vtt:
                    srt_path = vtt.with_suffix(".srt")
                    try:
                        run(["ffmpeg", "-y", "-i", str(vtt), str(srt_path)])
                        if srt_path.exists():
                            kind = "manual" if flag == "--write-sub" else "auto"
                            return srt_path, sub_langs, kind
                    except Exception:
                        vtt.rename(srt_path)
                        kind = "manual" if flag == "--write-sub" else "auto"
                        return srt_path, sub_langs, kind
            except Exception:
                continue

    # 3) TimedText fallback
    requested_langs = []
    if langs:
        for part in re.split(r"[,\s]+", langs):
            part = part.strip()
            if part and part not in ("*", "all"):
                requested_langs.append(part)

    candidates = list(dict.fromkeys(
        requested_langs +
        manual_langs +
        auto_langs +
        ["en-US", "en-GB", "en", "nl", "de", "fr", "es"]
    ))

    tt = timedtext_fallback(vid, wd, candidates)
    if tt:
        srt_path, used_lang = tt
        return srt_path, used_lang, "timedtext"

    # 4) Fail with filtered availability
    avail_msg = f"manual={manual_langs or []}, auto={auto_langs or []}"
    raise HTTPException(
        status_code=404,
        detail=(
            "No captions were found (manual+auto). "
            f"Available tracks (non-empty): {avail_msg}. "
            "Try passing langs='all' or a specific code you see listed."
        ),
    )

# ---------------------------
# PDF helper (best-effort)
# ---------------------------
def make_pdf_from_text(header: str, body: str, out_path: Path) -> bool:
    try:
        from fpdf import FPDF
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        for line in (header + body).split("\n"):
            pdf.multi_cell(0, 6, line)
        pdf.output(str(out_path))
        return out_path.exists()
    except Exception:
        return False

# ---------------------------
# API models
# ---------------------------
class Req(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB,all"
    keep_timestamps: bool = False

# ---------------------------
# Routes
# ---------------------------
@app.get("/health")
def health():
    return {"ok": True, "version": app.version}

@app.get("/file/{token}")
def get_file(token: str = FPath(...)):
    with _TOKEN_LOCK:
        item = _TOKEN_STORE.get(token)
        if not item:
            raise HTTPException(status_code=404, detail="Invalid or expired token.")
        if item.expires_at < time.time():
            _TOKEN_STORE.pop(token, None)
            raise HTTPException(status_code=404, detail="Link expired.")
        path = Path(item.path)
        if not path.exists():
            _TOKEN_STORE.pop(token, None)
            raise HTTPException(status_code=404, detail="File not found (expired).")

    def iterfile():
        with open(path, "rb") as f:
            yield from f

    headers = {
        "Content-Disposition": f"attachment; filename=\"{item.filename}\""
    }
    return StreamingResponse(iterfile(), media_type=item.mime, headers=headers)

@app.post("/transcript")
def transcript(req: Req, request: Request):
    url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={req.url_or_id}"
    vid = video_id(url)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)

        meta = get_info(url)
        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        published_at = meta.get("upload_date", "")
        if published_at and len(published_at) == 8:
            published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
        duration_s = int(meta.get("duration") or 0)

        # Fetch SRT (yt-dlp first, then TimedText)
        srt_path, lang_used, source_kind = try_fetch_srt(url, vid, req.langs, wd)

        # TXT (cleaned)
        txt_text = clean_srt_to_text(srt_path, keep_ts=req.keep_timestamps)
        txt_path = wd / f"{vid}_{lang_used}.txt"
        txt_path.write_text(txt_text, encoding="utf-8")

        # PDF (best effort)
        pdf_path = wd / f"{vid}_{lang_used}.pdf"
        header = f"{title} — {channel}\nhttps://www.youtube.com/watch?v={vid}\n\n"
        made_pdf = make_pdf_from_text(header, txt_text, pdf_path)

        # Register HTTP links
        txt_token = register_file(txt_path, "text/plain; charset=utf-8", txt_path.name, LINKS_TTL_SECONDS)
        srt_token = register_file(srt_path, "text/plain; charset=utf-8", srt_path.name, LINKS_TTL_SECONDS)
        pdf_token = register_file(pdf_path, "application/pdf", pdf_path.name, LINKS_TTL_SECONDS) if made_pdf else None

        txt_http_url = token_url(request, txt_token)
        srt_http_url = token_url(request, srt_token)
        pdf_http_url = token_url(request, pdf_token) if pdf_token else ""

        # Preview
        preview = txt_text[:PREVIEW_CHARS]
        truncated = len(txt_text) > len(preview)

        return {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "duration_pretty": pretty_duration(duration_s),
            "video_id": vid,
            "captions_lang": lang_used,
            "captions_kind": source_kind,  # "manual" | "auto" | "timedtext"
            "preview_text": preview,
            "truncated": truncated,
            "txt_http_url": txt_http_url,
            "srt_http_url": srt_http_url,
            "pdf_http_url": pdf_http_url,
            "links_expire_in_seconds": LINKS_TTL_SECONDS,
        }
