import re, json, base64, tempfile, subprocess
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Creator Transcript Fetcher", version="1.0.0")
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

def run(cmd, cwd: Path | None = None) -> str:
    """Run a shell command; optionally inside working directory `cwd`."""
    p = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(cwd) if cwd else None,
    )
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
        # remove index (e.g., "123")
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)
        # first timestamp for optional prefix
        m = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        # drop the timestamp line
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
    # de-duplicate immediate word repeats and tidy spacing
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

@app.post("/transcript")
def transcript(req: Req):
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

        # 2) download subtitles into temp folder (try manual then auto)
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

        # 3) ensure .srt exists (convert from .vtt if needed)
        srt = next(iter(wd.glob(f"{vid}*.srt")), None)
        vtt = None if srt else next(iter(wd.glob(f"{vid}*.vtt")), None)
        if not srt and vtt:
            run(["ffmpeg", "-y", "-i", str(vtt), str(vtt.with_suffix(".srt"))], cwd=wd)
            srt = vtt.with_suffix(".srt")
        if not srt or not srt.exists():
            raise HTTPException(status_code=404, detail="Failed to obtain subtitles.")

        # 4) clean transcript text
        txt = clean_srt_to_text(srt, keep_ts=req.keep_timestamps)
        txt_bytes = txt.encode("utf-8")
        srt_bytes = srt.read_bytes()

        # 5) optional PDF (ASCII-safe for fpdf 1.x)
        pdf_bytes = None
        try:
            from fpdf import FPDF

            def to_latin1(s: str) -> str:
                # drop characters fpdf 1.x can't encode
                return s.encode("latin-1", "ignore").decode("latin-1")

            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Helvetica", size=12)  # built-in core font

            header = f"{title} — {channel}\n{url}\n\n"
            for line in (header + txt).split("\n"):
                pdf.multi_cell(0, 6, to_latin1(line))

            pdf_path = wd / f"transcript_{vid}.pdf"
            pdf.output(str(pdf_path))
            pdf_bytes = pdf_path.read_bytes()
        except Exception:
            pdf_bytes = None

        # 6) data URLs for downloads
        txt_url = as_data_url("text/plain;charset=utf-8", txt_bytes)
        srt_url = as_data_url("text/plain;charset=utf-8", srt_bytes)
        pdf_url = as_data_url("application/pdf", pdf_bytes, b64=True)

        preview = txt[:2500]
        return {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "video_id": vid,
            "preview_text": preview,
            "truncated": len(txt) > len(preview),
            "txt_url": txt_url,
            "srt_url": srt_url,
            "pdf_url": pdf_url,
        }
