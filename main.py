import re, json, base64, tempfile, subprocess
from pathlib import Path
from time import time
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# ------------------------------
# In-memory file store (short-lived)
# ------------------------------
STORE_TTL_SECONDS = 60 * 60  # 1 hour (was 15 mins)

STORE: dict[str, dict] = {}  # fid -> {"bytes": b, "mime": str, "name": str, "exp": float}

def put_file(name: str, mime: str, b: bytes) -> str:
    fid = uuid4().hex
    STORE[fid] = {"bytes": b, "mime": mime, "name": name, "exp": time() + STORE_TTL_SECONDS}
    return fid

def get_file(fid: str):
    item = STORE.get(fid)
    if not item:
        return None
    if item["exp"] < time():
        STORE.pop(fid, None)
        return None
    return item

# ------------------------------
# App + models
# ------------------------------
app = FastAPI(title="Creator Transcript Fetcher", version="1.1.1")
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

# ------------------------------
# Helpers
# ------------------------------
def run(cmd, cwd: Path | None = None) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=str(cwd) if cwd else None)
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
    blocks = re.split(r"\n\s*\n", raw.strip())
    lines = []
    for blk in blocks:
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)  # remove index
        m = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        text = re.sub(
            r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$",
            "",
            blk,
        )
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        lines.append(f"{m.group(1)} {text}" if (keep_ts and m) else text)

    out = "\n".join(lines) if keep_ts else " ".join(lines)
    # de-dup immediate repeats + tidy punctuation spacing
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

def as_data_url(mime: str, content: bytes | None, b64: bool = False) -> str:
    if content is None:
        return ""
    if b64:
        return f"data:{mime};base64,{base64.b64encode(content).decode('ascii')}"
    from urllib.parse import quote
    return f"data:{mime},{quote(content.decode('utf-8'))}"

# ------------------------------
# API
# ------------------------------
@app.get("/file/{fid}")
def download_file(fid: str):
    """Short-lived HTTP download endpoint for GPT UI."""
    item = get_file(fid)
    if not item:
        raise HTTPException(status_code=404, detail="File expired or not found.")
    headers = {"Content-Disposition": f'attachment; filename="{item["name"]}"'}
    return Response(content=item["bytes"], media_type=item["mime"], headers=headers)

@app.post("/transcript")
def transcript(req: Req, request: Request):
    url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={req.url_or_id}"
    vid = video_id(url)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)

        # 1) metadata
        meta = get_meta(url)
        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        published_at = meta.get("upload_date", "")
        if published_at and len(published_at) == 8:
            published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
        duration_s = int(meta.get("duration") or 0)

        # 2) subtitles (manual first, then auto)
        base = [
            "yt-dlp",
            "--skip-download",
            "--sub-langs", req.langs,
            "--convert-subs", "srt",
            "--force-overwrites",
            "-o", "%(id)s.%(ext)s",
            url,
        ]
        ok = False
        for mode in ("--write-sub", "--write-auto-sub"):
            try:
                run(base[:1] + [mode] + base[1:], cwd=wd)
                ok = True
                break
            except Exception:
                continue
        if not ok:
            raise HTTPException(status_code=404, detail="No captions available for this video.")

        # 3) ensure .srt exists (convert vtt -> srt if needed)
        srt = next(iter(wd.glob(f"{vid}*.srt")), None)
        vtt = None if srt else next(iter(wd.glob(f"{vid}*.vtt")), None)
        if not srt and vtt:
            run(["ffmpeg", "-y", "-i", str(vtt), str(vtt.with_suffix(".srt"))], cwd=wd)
            srt = vtt.with_suffix(".srt")
        if not srt or not srt.exists():
            raise HTTPException(status_code=404, detail="Failed to obtain subtitles.")

        # 4) clean transcript
        txt = clean_srt_to_text(srt, keep_ts=req.keep_timestamps)
        txt_bytes = txt.encode("utf-8")
        srt_bytes = srt.read_bytes()

        # 5) PDF (ASCII-safe for fpdf 1.x)
        pdf_bytes = None
        try:
            from fpdf import FPDF

            def to_latin1(s: str) -> str:
                return s.encode("latin-1", "ignore").decode("latin-1")

            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Helvetica", size=12)

            header = f"{title} — {channel}\n{url}\n\n"
            for line in (header + txt).split("\n"):
                pdf.multi_cell(0, 6, to_latin1(line))

            pdf_path = wd / f"transcript_{vid}.pdf"
            pdf.output(str(pdf_path))
            pdf_bytes = pdf_path.read_bytes()
        except Exception:
            pdf_bytes = None

        # 6) Data URLs (good in Swagger) + short-lived HTTP links (work in GPT)
        txt_url = as_data_url("text/plain;charset=utf-8", txt_bytes)
        srt_url = as_data_url("text/plain;charset=utf-8", srt_bytes)
        pdf_url = as_data_url("application/pdf", pdf_bytes, b64=True) if pdf_bytes else ""

        base_url = str(request.base_url).rstrip("/")
        txt_http_url = f"{base_url}/file/{put_file(f'{vid}.txt', 'text/plain; charset=utf-8', txt_bytes)}"
        srt_http_url = f"{base_url}/file/{put_file(f'{vid}.srt', 'text/plain; charset=utf-8', srt_bytes)}"
        pdf_http_url = ""
        if pdf_bytes:
            pdf_http_url = f"{base_url}/file/{put_file(f'transcript_{vid}.pdf', 'application/pdf', pdf_bytes)}"

        preview = txt[:2500]
        return {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "video_id": vid,
            "preview_text": preview,
            "truncated": len(txt) > len(preview),
            # Data URLs (Swagger)
            "txt_url": txt_url,
            "srt_url": srt_url,
            "pdf_url": pdf_url,
            # HTTP links (GPT UI)
            "txt_http_url": txt_http_url,
            "srt_http_url": srt_http_url,
            "pdf_http_url": pdf_http_url,
            "links_expire_in_seconds": STORE_TTL_SECONDS
        }
