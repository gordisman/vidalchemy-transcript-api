import os, re, io, json, time, uuid, logging, tempfile, subprocess, threading
from pathlib import Path
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ------------------------------
# Config via env (safe defaults)
# ------------------------------
PUBLIC_BASE_URL  = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
FILE_TTL_SECONDS = int(os.getenv("FILE_TTL_SECONDS", "86400"))  # 24h default
PREVIEW_CHARS    = int(os.getenv("PREVIEW_CHARS", "3000"))
DEFAULT_LANGS    = os.getenv("DEFAULT_LANGS", "en,en-US,en-GB")  # GPT can override per-request

app = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False
)

log = logging.getLogger("uvicorn.error")

# -------------
# File storage
# -------------
file_lock = threading.Lock()
files = {}  # token -> {"bytes": b, "mime": str, "filename": str, "expires": float}

def _janitor():
    while True:
        now = time.time()
        with file_lock:
            expired = [t for t, m in files.items() if m["expires"] < now]
            for t in expired:
                files.pop(t, None)
        time.sleep(60)

threading.Thread(target=_janitor, daemon=True).start()

def register_file(contents: bytes, mime: str, filename: str) -> dict:
    token = uuid.uuid4().hex
    expires = time.time() + FILE_TTL_SECONDS
    with file_lock:
        files[token] = {"bytes": contents, "mime": mime, "filename": filename, "expires": expires}
    http_url = f"{PUBLIC_BASE_URL}/file/{token}" if PUBLIC_BASE_URL else f"/file/{token}"
    return {"token": token, "http_url": http_url, "expires_in_seconds": FILE_TTL_SECONDS}

# -------------
# Utilities
# -------------
def run(cmd):
    """Run a command; raise with full stderr on failure."""
    log.info(f"[yt-dlp] RUN: {' '.join(cmd)}")
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        err = (p.stderr or p.stdout or "").strip()
        log.error(f"[yt-dlp] ERROR ({p.returncode}): {err}")
        raise RuntimeError(err)
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
    s = int(seconds or 0)
    h, m, s = s // 3600, (s % 3600) // 60, s % 60
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def clean_srt_to_text(srt_path: Path, keep_ts: bool) -> str:
    raw = srt_path.read_text(encoding="utf-8", errors="ignore")
    blocks = re.split(r"\n\s*\n", raw.strip())
    lines = []
    for blk in blocks:
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)  # index
        m = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        text = re.sub(r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$", "", blk)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        lines.append(f"{m.group(1)} {text}" if keep_ts and m else text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)  # de-dupe repeats
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

def make_pdf_bytes(title: str, channel: str, url: str, txt: str) -> bytes | None:
    try:
        from fpdf import FPDF
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
        log.warning(f"PDF generation skipped: {e}")
        return None

def try_fetch_srt(url: str, vid: str, langs: str, wd: Path) -> tuple[Path, str]:
    """
    Fetch subtitles for a single video id.

    Improvements:
      - Inspect actual tracks and filter out empty entries (manual+auto).
      - Report only *real* available languages on failure.
      - Try wider sub-lang patterns.
      - Accept VTT fallback and convert to SRT as needed.
    Returns (srt_path, langs_used) on success, raises 404 with details otherwise.
    """
    # -- Step 1: Inspect info JSON
    info_raw = run(["yt-dlp", "-J", "--skip-download", url])
    info = json.loads(info_raw) or {}
    if isinstance(info, dict) and info.get("entries"):
        info = info["entries"][0]

    subs_all  = info.get("subtitles") or {}
    autos_all = info.get("automatic_captions") or {}

    # Keep only languages that have at least one entry with a URL
    def real_langs(d: dict) -> list[str]:
        real = []
        for code, formats in (d or {}).items():
            # formats is usually a list of dicts; keep only if there is a 'url'
            if any(isinstance(f, dict) and f.get("url") for f in (formats or [])):
                real.append(code)
        return sorted(real)

    manual_langs = real_langs(subs_all)
    auto_langs   = real_langs(autos_all)

    # -- Step 2: Prepare yt-dlp command
    base = [
        "yt-dlp", "--skip-download",
        # helps with some newer player responses
        "--extractor-args", "youtube:player_client=android",
        "--convert-subs", "srt",
        "--force-overwrites",
        "-o", "%(id)s.%(ext)s",
        url,
    ]

    # -- Step 3: Try multiple language patterns
    attempts: list[str] = []
    langs = (langs or "").strip()
    if langs:
        attempts.append(langs)              # user’s explicit request
        attempts.append(f"*,{langs}")       # anything + requested
    # good broad fallbacks
    attempts += ["all,-live_chat", "*", "all"]

    def find_first(*globs: str) -> Path | None:
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

                # Prefer SRT
                srt = find_first(f"{vid}*.srt")
                if srt:
                    return srt, sub_langs

                # Fallback: VTT -> SRT
                vtt = find_first(f"{vid}*.vtt")
                if vtt:
                    srt_path = vtt.with_suffix(".srt")
                    try:
                        run(["ffmpeg", "-y", "-i", str(vtt), str(srt_path)])
                        if srt_path.exists():
                            return srt_path, sub_langs
                    except Exception:
                        # As a last resort, rename VTT to .srt so downstream stays consistent
                        vtt.rename(srt_path)
                        return srt_path, sub_langs

            except Exception:
                # try next pattern
                continue

    # -- Step 4: Fail with *filtered* availability info so it’s trustworthy
    avail_msg = f"manual={manual_langs or []}, auto={auto_langs or []}"
    raise HTTPException(
        status_code=404,
        detail=(
            "No captions were found (manual+auto). "
            f"Available tracks (non-empty): {avail_msg}. "
            "Try passing langs='all' or a specific code you see listed (e.g., 'nl')."
        ),
    )


# -------------
# Models
# -------------
class Req(BaseModel):
    url_or_id: str
    langs: str = DEFAULT_LANGS
    keep_timestamps: bool = False

# -------------
# Endpoints
# -------------
@app.get("/health")
def health():
    return {"ok": True, "file_count": len(files), "ttl_seconds": FILE_TTL_SECONDS}

@app.get("/file/{token}")
def get_file(token: str):
    with file_lock:
        meta = files.get(token)
        if not meta:
            raise HTTPException(status_code=404, detail="File expired or not found.")
        if meta["expires"] < time.time():
            files.pop(token, None)
            raise HTTPException(status_code=404, detail="File expired.")
        data = meta["bytes"]; mime = meta["mime"]; filename = meta["filename"]
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=data, media_type=mime, headers=headers)

@app.post("/transcript")
def transcript(req: Req):
    # Build full YouTube URL if only id was given
    url = req.url_or_id if req.url_or_id.startswith("http") \
        else f"https://www.youtube.com/watch?v={req.url_or_id}"
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

        # Subtitles
        srt_path, langs_used = try_fetch_srt(url, vid, req.langs, wd)

        # Text
        txt = clean_srt_to_text(srt_path, keep_ts=req.keep_timestamps)
        preview   = txt[:PREVIEW_CHARS]
        truncated = len(txt) > len(preview)

        # Files
        txt_info = register_file(txt.encode("utf-8"), "text/plain; charset=utf-8", f"{vid}.txt")
        srt_info = register_file(srt_path.read_bytes(), "text/plain; charset=utf-8", f"{vid}.srt")

        pdf_http_url = ""
        pdf_bytes = make_pdf_bytes(title, channel, url, txt)
        if pdf_bytes:
            pdf_info = register_file(pdf_bytes, "application/pdf", f"{vid}.pdf")
            pdf_http_url = pdf_info["http_url"]

        return {
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
            "pdf_http_url": pdf_http_url,
            "links_expire_in_seconds": FILE_TTL_SECONDS,
            "langs_requested": req.langs,
            "langs_used": langs_used,
        }
