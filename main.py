import os
import re
import time
import base64
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from yt_dlp import YoutubeDL
from urllib.error import HTTPError

# -----------------------------
# Config
# -----------------------------
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
EXPIRES_IN_SECONDS = int(os.getenv("FILE_EXPIRES_SECONDS", "86400"))  # default 24h
PORT = int(os.getenv("PORT", "8000"))

# -----------------------------
# App
# -----------------------------
app = FastAPI(title="Creator Transcript Fetcher", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Ephemeral file storage
# -----------------------------
FILES: Dict[str, Dict] = {}


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
    base = PUBLIC_BASE_URL or "https://vidalchemy-transcript-api-production.up.railway.app"
    return f"{base}/file/{token}"

# -----------------------------
# Utility
# -----------------------------
def pretty_duration(seconds: int) -> str:
    if not seconds:
        return "0s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def normalize_lang(code: str) -> str:
    code = code.strip().lower()
    if "-" in code:
        code = code.split("-")[0]
    if code.endswith("orig"):
        code = code.replace("orig", "")
    return code.strip()


def ordered_langs(user_langs: str) -> List[str]:
    pref = ["en", "en-us", "en-gb"]
    if not user_langs:
        return pref + ["all"]

    parts = [normalize_lang(p) for p in user_langs.split(",") if p.strip()]
    seen, out = set(), []
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
    """Fetch URL with retry/backoff on 429 errors."""
    delays = [2, 5, 10]
    last_err = None
    for attempt, delay in enumerate(delays, start=1):
        try:
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
        except HTTPError as e:
            if e.code == 429:
                last_err = e
                if attempt < len(delays):
                    time.sleep(delay)
                    continue
            raise
        except Exception as e:
            last_err = e
            break
    if last_err:
        raise last_err


def vtt_to_srt_bytes(vtt: bytes) -> bytes:
    text = vtt.decode("utf-8", errors="ignore")
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("WEBVTT")]
    out_lines, buf, idx = [], [], 1

    def flush():
        nonlocal idx, buf
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
        buf.clear()

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
    lines = []
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
            lines.append(f"{m.group(1)} {text}")
        else:
            lines.append(text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

# -----------------------------
# Preview logic
# -----------------------------
def build_preview(full_text: str, max_chars: int = 3000, min_sentences: int = 5, char_target: int = 800):
    sentences = re.split(r'(?<=[.!?])\s+', full_text)
    preview, count = "", 0

    for s in sentences:
        if not s.strip():
            continue
        preview += s.strip() + " "
        count += 1
        if count >= min_sentences and len(preview) >= char_target:
            break

    if len(preview) > max_chars:
        preview = preview[:max_chars]

    truncated = len(full_text) > len(preview)
    return preview.strip(), truncated

# -----------------------------
# Caption track selection
# -----------------------------
def pick_caption_track(info: dict, langs: List[str]) -> Optional[dict]:
    subs, autos = info.get("subtitles") or {}, info.get("automatic_captions") or {}

    for lang in langs:
        if lang == "all":
            for d in (subs, autos):
                for tracks in d.values():
                    if tracks:
                        return tracks[0]
        if lang in subs and subs[lang]:
            return subs[lang][0]
        if lang in autos and autos[lang]:
            return autos[lang][0]
    return None

# -----------------------------
# Schemas
# -----------------------------
class Req(BaseModel):
    url_or_id: str
    langs: str
    keep_timestamps: bool

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True, "egress_to_youtube": True, "expires_in_seconds_default": EXPIRES_IN_SECONDS}


@app.get("/probe")
def probe(url: str):
    info = yt_info(url)
    return {
        "info": {
            "title": info.get("title"),
            "duration": info.get("duration"),
            "subtitles_keys": list((info.get("subtitles") or {}).keys()),
            "auto_captions_keys": list((info.get("automatic_captions") or {}).keys()),
        }
    }


@app.get("/file/{token}")
def getFile(token: str):
    _purge_expired()
    meta = FILES.get(token)
    if not meta:
        raise HTTPException(status_code=404, detail="File expired or not found")
    path = Path(meta["path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="File missing")
    with open(path, "rb") as f:
        data = f.read()
    return Response(
        content=data,
        media_type=meta["mime"],
        headers={"Content-Disposition": f"attachment; filename={meta['filename']}"},
    )


@app.post("/transcript")
def fetchTranscript(req: Req):
    try:
        langs = ordered_langs(req.langs)
        info = yt_info(req.url_or_id)
        track = pick_caption_track(info, langs)

        if not track:
            return {
                "ok": False,
                "error": "No captions found",
                "available_langs": list((info.get("subtitles") or {}).keys()) +
                                   list((info.get("automatic_captions") or {}).keys()),
                "tried_langs": langs,
            }

        vtt_url = best_caption_url([track])
        if not vtt_url:
            return {"ok": False, "error": "No caption URL available"}

        vtt_bytes = http_fetch(vtt_url)
        srt_bytes = vtt_to_srt_bytes(vtt_bytes)
        full_text = clean_srt_text(srt_bytes, req.keep_timestamps)

        preview_text, truncated = build_preview(full_text)

        # Save files
        base = Path("/tmp")
        txt_path, srt_path, pdf_path = base / f"{_tok()}.txt", base / f"{_tok()}.srt", base / f"{_tok()}.pdf"
        txt_path.write_text(full_text, encoding="utf-8")
        srt_path.write_bytes(srt_bytes)

        # Try PDF
        pdf_token = None
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            for line in full_text.split("\n"):
                pdf.multi_cell(0, 10, line)
            pdf.output(str(pdf_path))
            pdf_token = _store_file(pdf_path, "application/pdf", "transcript.pdf")
        except Exception:
            pdf_token = None

        txt_token = _store_file(txt_path, "text/plain", "transcript.txt")
        srt_token = _store_file(srt_path, "application/x-subrip", "transcript.srt")

        return {
            "ok": True,
            "title": info.get("title"),
            "channel": info.get("uploader"),
            "published_at": info.get("upload_date"),
            "duration_s": info.get("duration"),
            "duration_pretty": pretty_duration(info.get("duration", 0)),
            "video_id": extract_video_id(req.url_or_id),
            "captions_kind": "manual" if track in sum(info.get("subtitles", {}).values(), []) else "auto",
            "captions_lang": track.get("lang", "unknown"),
            "preview_text": preview_text,
            "truncated": truncated,
            "txt_http_url": _file_url(txt_token),
            "srt_http_url": _file_url(srt_token),
            "pdf_http_url": _file_url(pdf_token) if pdf_token else None,
            "links_expire_in_seconds": EXPIRES_IN_SECONDS,
            "links_expire_human": f"{EXPIRES_IN_SECONDS // 3600}h",
        }

    except Exception as e:
        return {
            "ok": False,
            "error": f"Failed while processing captions: {str(e)}",
            "available_langs": [],
            "tried_langs": ordered_langs(req.langs),
        }
