# Transcript API (FastAPI) — Railway Starter Kit

Here are your three files in clean copy‑blocks. Copy each one separately when creating files in GitHub.

---

## `main.py`

```python
import os, re, json, base64, tempfile, subprocess
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Creator Transcript Fetcher", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False)

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
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)
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
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

def as_data_url(mime: str, content: bytes, b64: bool = False) -> str:
    if b64:
        return f"data:{mime};base64,{base64.b64encode(content).decode('ascii')}"
    else:
        from urllib.parse import quote
        return f"data:{mime},{quote(content.decode('utf-8'))}"

@app.post("/transcript")
def transcript(req: Req):
    url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={req.url_or_id}"
    vid = video_id(url)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)
        meta = get_meta(url)
        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        published_at = meta.get("upload_date", "")
        if published_at and len(published_at) == 8:
            published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"
        duration_s = int(meta.get("duration") or 0)

        base = [
            "yt-dlp", "--skip-download",
            "--sub-langs", req.langs,
            "--convert-subs", "srt",
            "--force-overwrites",
            "-o", "%(id)s.%(ext)s",
            url,
        ]
        ok = False
        for mode in ("--write-sub", "--write-auto-sub"):
            try:
                run(base[:1] + [mode] + base[1:])
                ok = True
                break
            except Exception:
                continue
        if not ok:
            raise HTTPException(status_code=404, detail="No captions available for this video.")

        srt = next(iter(wd.glob(f"{vid}*.srt")), None)
        vtt = None if srt else next(iter(wd.glob(f"{vid}*.vtt")), None)
        if not srt and vtt:
            run(["ffmpeg", "-y", "-i", str(vtt), str(vtt.with_suffix(".srt"))])
            srt = vtt.with_suffix(".srt")
        if not srt or not srt.exists():
            raise HTTPException(status_code=404, detail="Failed to obtain subtitles.")

        txt = clean_srt_to_text(srt, keep_ts=req.keep_timestamps)
        txt_bytes = txt.encode("utf-8")
        srt_bytes = srt.read_bytes()

        pdf_bytes = None
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            header = f"{title} — {channel}\n{url}\n\n"
            for chunk in (header + txt).split("\n"):
                pdf.multi_cell(0, 6, chunk)
            pdf_path = wd / f"transcript_{vid}.pdf"
            pdf.output(str(pdf_path))
            pdf_bytes = pdf_path.read_bytes()
        except Exception:
            pdf_bytes = None

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
            "txt_url": txt_url,
            "srt_url": srt_url,
            "pdf_url": pdf_url,
        }
```

---

## `requirements.txt`

```text
fastapi==0.115.0
uvicorn[standard]==0.30.6
yt-dlp==2025.01.12
fpdf==1.7.2
```

---

## `Dockerfile`

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=8000
EXPOSE 8000
CMD ["sh","-c","uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
```
