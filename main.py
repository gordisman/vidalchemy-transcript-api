import os
import re
import json
import time
import base64
import shutil
import secrets
import tempfile
import subprocess
from pathlib import Path
from typing import Optional, Tuple, List, Dict

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import FileResponse, PlainTextResponse

# -----------------------------
# Config & simple file registry
# -----------------------------
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
FILE_TTL_SECONDS = int(os.environ.get("FILE_TTL_SECONDS", "86400"))  # 24h default
FILES_DIR = Path("/tmp/vafiles")
FILES_DIR.mkdir(parents=True, exist_ok=True)

# token -> {path: Path, mime: str, filename: str, expires_at: float}
TOKENS: Dict[str, Dict] = {}

def now() -> float:
    return time.time()

def purge_expired_tokens() -> None:
    to_del = [t for t, meta in TOKENS.items() if meta["expires_at"] <= now()]
    for t in to_del:
        try:
            p: Path = TOKENS[t]["path"]
            if p.exists():
                p.unlink(missing_ok=True)
        except Exception:
            pass
        TOKENS.pop(t, None)

def register_file(path: Path, mime: str, filename: str) -> str:
    purge_expired_tokens()
    token = secrets.token_hex(16)
    TOKENS[token] = {
        "path": path,
        "mime": mime,
        "filename": filename,
        "expires_at": now() + FILE_TTL_SECONDS,
    }
    if not PUBLIC_BASE_URL:
        # Fallback: relative path (works in Swagger), but for GPT Action you should set PUBLIC_BASE_URL.
        return f"/file/{token}"
    return f"{PUBLIC_BASE_URL}/file/{token}"


# -------------
# FastAPI app
# -------------
app = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

# -------------
# Models
# -------------
class Req(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB"
    keep_timestamps: bool = False


# -------------
# Helpers
# -------------
def run(cmd: List[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr or p.stdout)
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

def clean_srt_to_text(srt_path: Path, keep_ts: bool) -> str:
    raw = srt_path.read_text(encoding="utf-8", errors="ignore")
    # Split blocks on blank lines
    blocks = re.split(r"\n\s*\n", raw.strip())
    lines = []
    for blk in blocks:
        # Remove leading index number
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)
        # Remove timing line
        m = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        text = re.sub(r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$", "", blk)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and m:
            lines.append(f"{m.group(1)} {text}")
        else:
            lines.append(text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    # De-dupe simple repeated words caused by subtitle overlap
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out


# -----------------------------
# TimedText XML → SRT converter
# -----------------------------
def xml_to_srt(xml_text: str, out_srt_path: Path) -> bool:
    """
    Convert YouTube TimedText XML/TTML (<transcript><text ...>) to SRT.
    Returns True on success.
    """
    try:
        import html
        import xml.etree.ElementTree as ET

        root = ET.fromstring(xml_text)
        entries = []
        idx = 1

        for node in root.iter("text"):
            start = float(node.attrib.get("start", "0"))
            dur = float(node.attrib.get("dur", "0"))
            end = start + dur

            def fmt(t):
                h = int(t // 3600)
                m = int((t % 3600) // 60)
                s = int(t % 60)
                ms = int(round((t - int(t)) * 1000))
                return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

            text = "".join(node.itertext())
            text = html.unescape(text).replace("\n", " ").strip()
            if not text:
                continue

            entries.append(f"{idx}\n{fmt(start)} --> {fmt(end)}\n{text}\n")
            idx += 1

        if not entries:
            return False

        out_srt_path.write_text("\n".join(entries), encoding="utf-8")
        return True
    except Exception:
        return False


# -----------------------------
# TimedText fallback (VTT + XML)
# -----------------------------
def timedtext_fallback(vid: str, wd: Path, langs_to_try: List[str]) -> Optional[Tuple[Path, str]]:
    """
    Fetch captions directly from the YouTube TimedText endpoint.
    Supports BOTH VTT and XML/TTML. Returns (srt_path, lang_used) on success, else None.
    """
    import requests

    def tt_url(lang: str, asr: bool, fmt: Optional[str]) -> str:
        base = f"https://www.youtube.com/api/timedtext?v={vid}&lang={lang}"
        if asr:
            base += "&kind=asr"
        if fmt:
            base += f"&fmt={fmt}"
        return base

    def save_convert_vtt(vtt_text: str, lang: str) -> Optional[Path]:
        vtt_path = wd / f"{vid}.{lang}.vtt"
        vtt_path.write_text(vtt_text, encoding="utf-8")
        srt_path = vtt_path.with_suffix(".srt")
        try:
            run(["ffmpeg", "-y", "-i", str(vtt_path), str(srt_path)])
            if srt_path.exists():
                return srt_path
        except Exception:
            # Worst case, rename (not perfect, but gives user something)
            try:
                vtt_path.rename(srt_path)
                return srt_path
            except Exception:
                return None
        return None

    for lang in langs_to_try:
        for asr in (False, True):
            # Try VTT
            try:
                r = requests.get(tt_url(lang, asr, "vtt"), timeout=12)
                if r.status_code == 200 and r.text and "WEBVTT" in r.text[:1000]:
                    srt_path = save_convert_vtt(r.text, lang)
                    if srt_path:
                        return srt_path, lang
            except Exception:
                pass

            # Try XML/TTML -> SRT
            try:
                r = requests.get(tt_url(lang, asr, None), timeout=12)
                if r.status_code == 200 and r.text and "<transcript" in r.text[:1000]:
                    srt_path = wd / f"{vid}.{lang}.srt"
                    if xml_to_srt(r.text, srt_path):
                        return srt_path, lang
            except Exception:
                pass

    return None


# -----------------------------
# yt-dlp caption fetch
# -----------------------------
def fetch_with_ytdlp(url: str, langs: str, wd: Path) -> Tuple[Optional[Path], str, str]:
    """
    Attempt to fetch captions with yt-dlp.
    Returns (srt_path, lang_used, kind) where kind in {"manual","auto"}; or (None,"","") on failure.
    """
    # Normalize special cases
    langs = (langs or "").strip()
    if langs.lower() in ("", "all"):
        sub_langs = "all"
    else:
        sub_langs = langs

    base = [
        "yt-dlp",
        "--skip-download",
        "--convert-subs", "srt",
        "--force-overwrites",
        "-o", "%(id)s.%(ext)s",
        url,
    ]

    # Try MANUAL first
    try:
        run(["yt-dlp", "--sub-langs", sub_langs, "--write-sub"] + base[2:])
        # pick first .srt in wd
        srt = next(iter(wd.glob("*.srt")), None)
        if srt:
            # Try to infer language from filename: <videoid>.<lang>.srt or <lang>.srt
            m = re.search(r"\.([a-zA-Z0-9-]{1,10})\.srt$", srt.name)
            lang_used = m.group(1) if m else ""
            return srt, (lang_used or ""), "manual"
    except Exception:
        pass

    # Try AUTO
    try:
        run(["yt-dlp", "--sub-langs", sub_langs, "--write-auto-sub"] + base[2:])
        srt = next(iter(wd.glob("*.srt")), None)
        if srt:
            m = re.search(r"\.([a-zA-Z0-9-]{1,10})\.srt$", srt.name)
            lang_used = m.group(1) if m else ""
            return srt, (lang_used or ""), "auto"
    except Exception:
        pass

    return None, "", ""


def pretty_seconds(total: int) -> str:
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# -------------
# Endpoints
# -------------
@app.get("/health")
def health():
    purge_expired_tokens()
    return {"ok": True, "expires_in_seconds_default": FILE_TTL_SECONDS}

@app.get("/file/{token}")
def get_file(token: str):
    purge_expired_tokens()
    meta = TOKENS.get(token)
    if not meta:
        raise HTTPException(404, "Link expired or invalid.")
    path: Path = meta["path"]
    if not path.exists():
        raise HTTPException(404, "File no longer exists.")
    # Content-Disposition: attachment; filename="..."
    headers = {"Content-Disposition": f'attachment; filename="{meta["filename"]}"'}
    return FileResponse(path, media_type=meta["mime"], headers=headers)


@app.post("/transcript")
def transcript(req: Req):
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

        # Try yt-dlp (manual → auto)
        srt_path, lang_used, kind = fetch_with_ytdlp(url, req.langs, wd)

        # If yt-dlp failed, try TimedText fallback
        if not srt_path:
            # Build lang list to try
            langs_field = (req.langs or "").strip().lower()
            if langs_field in ("", "all"):
                langs_to_try = ["en", "en-US", "en-GB", "nl", "fr", "de", "es", "pt", "it", "ja", "ko", "zh", "zh-TW", "hi", "ar", "ru"]
            else:
                # expand wildcards like "en,*"
                parts = [p.strip() for p in langs_field.split(",") if p.strip()]
                langs_to_try = []
                for p in parts:
                    if p == "*":
                        # add a broad set
                        langs_to_try += ["en", "en-US", "en-GB", "nl", "fr", "de", "es", "pt", "it", "ja", "ko", "zh", "zh-TW", "hi", "ar", "ru"]
                    else:
                        langs_to_try.append(p)
            tt = timedtext_fallback(vid, wd, langs_to_try)
            if tt:
                srt_path, lang_used = tt
                kind = "timedtext"

        if not srt_path or not srt_path.exists():
            raise HTTPException(
                status_code=404,
                detail="No captions were found (manual/auto/TimedText). Try a different video or pass a specific language (e.g., 'nl')."
            )

        # Generate .txt from srt
        txt = clean_srt_to_text(srt_path, keep_ts=req.keep_timestamps)
        txt_path = wd / f"{vid}.txt"
        txt_path.write_text(txt, encoding="utf-8")

        # PDF (best effort)
        pdf_path = None
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            header = f"{title} — {channel}\nhttps://www.youtube.com/watch?v={vid}\n\n"
            for line in (header + txt).split("\n"):
                pdf.multi_cell(0, 6, line)
            pdf_path = wd / f"{vid}.pdf"
            pdf.output(str(pdf_path))
        except Exception:
            pdf_path = None  # ignore PDF failure

        # Move files to persisted dir and register tokens
        persisted_txt = FILES_DIR / f"{vid}.txt"
        persisted_srt = FILES_DIR / f"{vid}.srt"
        shutil.copyfile(txt_path, persisted_txt)
        shutil.copyfile(srt_path, persisted_srt)

        txt_url = register_file(persisted_txt, "text/plain", f"{vid}.txt")
        srt_url = register_file(persisted_srt, "text/plain", f"{vid}.srt")

        pdf_url = ""
        if pdf_path and pdf_path.exists():
            persisted_pdf = FILES_DIR / f"{vid}.pdf"
            shutil.copyfile(pdf_path, persisted_pdf)
            pdf_url = register_file(persisted_pdf, "application/pdf", f"{vid}.pdf")

        preview = txt[:2500]
        return {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "duration_pretty": pretty_seconds(duration_s),
            "video_id": vid,
            "captions_lang": lang_used or "",
            "captions_kind": kind or "",
            "preview_text": preview,
            "truncated": len(txt) > len(preview),
            "txt_http_url": txt_url,
            "srt_http_url": srt_url,
            "pdf_http_url": pdf_url,
            "links_expire_in_seconds": FILE_TTL_SECONDS,
        }
