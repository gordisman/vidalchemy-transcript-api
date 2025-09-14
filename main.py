import os
import re
import io
import json
import time
import base64
import shutil
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from yt_dlp import YoutubeDL

# -----------------------------
# Config (via env or sane defaults)
# -----------------------------
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
EXPIRES_IN_SECONDS = int(os.getenv("FILE_EXPIRES_SECONDS", "86400"))  # 24h default
PORT = int(os.getenv("PORT", "8000"))

# -----------------------------
# App
# -----------------------------
app = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Ephemeral file storage with tokens
# -----------------------------
FILES: Dict[str, Dict] = {}  # token -> {path, mime, filename, expires_at}

def _tok() -> str:
    return base64.urlsafe_b64encode(os.urandom(16)).decode("ascii").rstrip("=")

def _now() -> int:
    return int(time.time())

def _purge_expired() -> None:
    dead = []
    for t, meta in FILES.items():
        if meta["expires_at"] <= _now() or not Path(meta["path"]).exists():
            dead.append(t)
    for t in dead:
        try:
            Path(FILES[t]["path"]).unlink(missing_ok=True)
        except Exception:
            pass
        FILES.pop(t, None)

def _store_file(path: Path, mime: str, filename: str) -> str:
    _purge_expired()
    token = _tok()
    FILES[token] = {
        "path": str(path),
        "mime": mime,
        "filename": filename,
        "expires_at": _now() + EXPIRES_IN_SECONDS,
    }
    return token

def _file_url(token: str) -> str:
    if not PUBLIC_BASE_URL:
        return ""
    return f"{PUBLIC_BASE_URL}/file/{token}"

# -----------------------------
# Utility
# -----------------------------
def pretty_duration(seconds: int) -> str:
    if seconds <= 0:
        return "0s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def normalize_lang(code: str) -> str:
    return code.strip().lower()

def ordered_langs(user_langs: str) -> List[str]:
    pref = ["en", "en-us", "en-gb"]
    if not user_langs:
        return pref + ["all"]

    parts = [normalize_lang(p) for p in user_langs.split(",") if p.strip()]
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    if "all" not in out:
        out.append("all")
    return out

def extract_video_id(url_or_id: str) -> str:
    u = url_or_id.strip()
    m = re.search(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})", u)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", u):
        return u
    return u

def yt_info(url: str) -> dict:
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "geo_bypass": True,
        "cachedir": False,
        "ignoreerrors": True,
        "noprogress": True,
        "simulate": True,
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
    if info and info.get("entries"):
        info = info["entries"][0]
    if not isinstance(info, dict):
        raise Exception("Failed to fetch video metadata.")
    return info

def pick_caption_track(info: dict, pref_langs: List[str]) -> Tuple[str, str, str]:
    subs = info.get("subtitles") or {}
    autos = info.get("automatic_captions") or {}

    manual_langs = {normalize_lang(k): k for k in subs.keys()}
    auto_langs   = {normalize_lang(k): k for k in autos.keys()}

    for want in pref_langs:
        if want == "all":
            for norm, raw in manual_langs.items():
                cand = subs.get(raw) or []
                url = best_caption_url(cand)
                if url:
                    return ("manual", raw, url)
            for norm, raw in auto_langs.items():
                cand = autos.get(raw) or []
                url = best_caption_url(cand)
                if url:
                    return ("auto", raw, url)
        else:
            raw = manual_langs.get(want)
            if raw:
                url = best_caption_url(subs.get(raw) or [])
                if url:
                    return ("manual", raw, url)
            raw = auto_langs.get(want)
            if raw:
                url = best_caption_url(autos.get(raw) or [])
                if url:
                    return ("auto", raw, url)

    for norm, raw in auto_langs.items():
        url = best_caption_url(autos.get(raw) or [])
        if url:
            return ("auto", raw, url)

    raise Exception("No captions were found in requested/available languages.")

def best_caption_url(tracks: List[dict]) -> Optional[str]:
    if not tracks:
        return None
    for t in tracks:
        if (t.get("ext") or "").lower() == "vtt" and t.get("url"):
            return t["url"]
    for t in tracks:
        if t.get("url"):
            return t["url"]
    return None

def http_fetch(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()

def vtt_to_srt_bytes(vtt: bytes) -> bytes:
    text = vtt.decode("utf-8", errors="ignore")
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("WEBVTT")]
    out_lines: List[str] = []
    idx = 1
    buf: List[str] = []

    def flush():
        nonlocal idx, buf, out_lines
        if not buf:
            return
        head = buf[0].replace(".", ",")
        out_lines.append(str(idx))
        out_lines.append(head)
        for tline in buf[1:]:
            if "-->" not in tline:
                out_lines.append(re.sub(r"<[^>]+>", "", tline))
        out_lines.append("")
        idx += 1
        buf = []

    for ln in lines:
        if re.match(r"^\s*$", ln):
            flush()
            continue
        if "-->" in ln:
            if buf:
                flush()
            buf = [ln.strip()]
        else:
            if buf:
                buf.append(ln)
    flush()
    return ("\n".join(out_lines)).encode("utf-8")

def clean_srt_text(srt_bytes: bytes, keep_ts: bool) -> str:
    raw = srt_bytes.decode("utf-8", errors="ignore")
    blocks = re.split(r"\n\s*\n", raw.strip())
    lines: List[str] = []
    for blk in blocks:
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)
        m = re.search(r"(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk)
        text = re.sub(
            r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$",
            "",
            blk,
        )
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and m:
            ts = m.group(1)
            lines.append(f"{ts} {text}")
        else:
            lines.append(text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

# -----------------------------
# Schemas
# -----------------------------
class Req(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB,all"
    keep_timestamps: bool = False

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "egress_to_youtube": True,
        "expires_in_seconds_default": EXPIRES_IN_SECONDS,
    }

@app.get("/probe")
def probe(url: str):
    vid = extract_video_id(url)
    full = f"https://www.youtube.com/watch?v={vid}" if not url.startswith("http") else url
    info = yt_info(full)
    manual = sorted((info.get("subtitles") or {}).keys())
    auto = sorted((info.get("automatic_captions") or {}).keys())
    samples = []
    for test in ["en", "en-US", "nl"]:
        if test in (info.get("subtitles") or {}):
            samples.append({"lang": test, "status": 200})
        elif test in (info.get("automatic_captions") or {}):
            samples.append({"lang": test, "status": 200})
        else:
            samples.append({"lang": test, "status": 404})
    return {
        "info": {
            "title": info.get("title"),
            "duration": info.get("duration") or 0,
            "subtitles_keys": manual,
            "auto_captions_keys": auto,
        },
        "timedtext_samples": samples,
    }

@app.get("/file/{token}")
def get_file(token: str):
    meta = FILES.get(token)
    if not meta:
        raise HTTPException(404, "Expired or invalid file token.")
    if meta["expires_at"] <= _now():
        _purge_expired()
        raise HTTPException(404, "Expired or invalid file token.")
    p = Path(meta["path"])
    if not p.exists():
        _purge_expired()
        raise HTTPException(404, "File not found.")
    headers = {"Content-Disposition": f"attachment; filename={json.dumps(meta['filename'])}"}
    return Response(p.read_bytes(), headers=headers, media_type=meta["mime"])

@app.post("/transcript")
def transcript(req: Req):
    vid = extract_video_id(req.url_or_id)
    url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={vid}"

    try:
        info = yt_info(url)
    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed to fetch metadata: {str(e)}",
            "available_langs": [],
            "tried_langs": ordered_langs(req.langs),
        }

    title = info.get("title", "")
    channel = info.get("channel") or info.get("uploader", "")
    published_at = info.get("upload_date", "")
    if published_at and len(published_at) == 8:
        published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
    duration_s = int(info.get("duration") or 0)

    subs = info.get("subtitles") or {}
    autos = info.get("automatic_captions") or {}
    available_langs = sorted(set(list(subs.keys()) + list(autos.keys())))

    try:
        pref = ordered_langs(req.langs)
        kind, lang_raw, track_url = pick_caption_track(info, pref)
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "available_langs": available_langs,
            "tried_langs": ordered_langs(req.langs),
        }

    try:
        vtt_bytes = http_fetch(track_url)
        srt_bytes = vtt_to_srt_bytes(vtt_bytes)
        text = clean_srt_text(srt_bytes, keep_ts=req.keep_timestamps)
        text_bytes = text.encode("utf-8")

        work = Path("/tmp") / f"cap_{vid}_{int(time.time())}"
        work.mkdir(parents=True, exist_ok=True)

        srt_path = work / f"{vid}_{lang_raw}.srt"
        srt_path.write_bytes(srt_bytes)

        txt_path = work / f"{vid}_{lang_raw}.txt"
        txt_path.write_bytes(text_bytes)

        pdf_path = work / f"{vid}_{lang_raw}.pdf"
        pdf_bytes = None
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            header = f"{title}\n{channel} · {published_at}\nhttps://www.youtube.com/watch?v={vid}\n\n"
            for line in (header + text).split("\n"):
                pdf.multi_cell(0, 6, line)
            pdf.output(str(pdf_path))
            pdf_bytes = pdf_path.read_bytes()
        except Exception:
            pdf_bytes = None

        srt_tok = _store_file(srt_path, "text/plain", srt_path.name)
        txt_tok = _store_file(txt_path, "text/plain", txt_path.name)
        pdf_tok = _store_file(pdf_path, "application/pdf", pdf_path.name) if pdf_bytes else None

        return {
            "ok": True,
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "duration_pretty": pretty_duration(duration_s),
            "video_id": vid,
            "captions_kind": kind,
            "captions_lang": lang_raw,
            "preview_text": text[:3000],
            "truncated": len(text) > 3000,
            "txt_http_url": _file_url(txt_tok),
            "srt_http_url": _file_url(srt_tok),
            "pdf_http_url": _file_url(pdf_tok) if pdf_tok else "",
            "links_expire_in_seconds": EXPIRES_IN_SECONDS,
            "links_expire_human": f"{EXPIRES_IN_SECONDS//3600}h",
        }

    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed while processing captions: {str(e)}",
            "available_langs": available_langs,
            "tried_langs": ordered_langs(req.langs),
        }
