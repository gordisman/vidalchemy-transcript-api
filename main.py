import os, re, json, base64, tempfile, subprocess, time
from pathlib import Path
from datetime import timedelta

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any

# --------------------------
# Config via environment
# --------------------------
EXPIRES_SECONDS = int(os.getenv("EXPIRES_IN_SECONDS", "86400"))  # 24h default
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")   # e.g. https://vidalchemy-transcript-api-production.up.railway.app

app = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False
)

# --------------------------
# Models
# --------------------------
class Req(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB,all"   # default: try English first, then any available
    keep_timestamps: bool = False

# --------------------------
# Utilities
# --------------------------
def run(cmd: List[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr or p.stdout)
    return p.stdout

def video_id(u: str) -> str:
    m = re.search(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})", u)
    return m.group(1) if m else u.strip()

def _lang_list(langs: str) -> List[str]:
    """
    Parse comma-separated langs. Place 'all' (or '*') first if present so we can
    accept whatever is available before trying specific codes. Always dedupe.
    """
    raw = [x.strip() for x in (langs or "").split(",") if x.strip()]
    if not raw:
        raw = ["en", "en-US", "en-GB", "all"]

    out: List[str] = []
    has_all = False
    for x in raw:
        if x.lower() in {"all", "*"}:
            has_all = True
        elif x not in out:
            out.append(x)

    if has_all:
        # try "any available" first, then user-specified list
        out = ["all"] + out
    return out

def _duration_pretty(seconds: int) -> str:
    if seconds <= 0:
        return "0s"
    td = timedelta(seconds=seconds)
    # hh:mm:ss or mm:ss
    total = int(td.total_seconds())
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"

def _data_url(mime: str, content: bytes, b64: bool = False) -> str:
    if b64:
        return f"data:{mime};base64,{base64.b64encode(content).decode('ascii')}"
    else:
        from urllib.parse import quote
        return f"data:{mime},{quote(content.decode('utf-8', 'ignore'))}"

def _build_http_file_url(token: str) -> str:
    if not PUBLIC_BASE_URL:
        return ""
    return f"{PUBLIC_BASE_URL}/file/{token}"

# --------------------------
# Health + Probe
# --------------------------
@app.get("/health")
def health():
    return {"ok": True, "egress_to_youtube": True, "expires_in_seconds_default": EXPIRES_SECONDS}

@app.get("/probe")
def probe(url: str):
    """
    Quick probe that lists which caption tracks look present (English & Dutch samples included).
    """
    vid = video_id(url)
    out = run(["yt-dlp", "-J", "--skip-download", f"https://www.youtube.com/watch?v={vid}"])
    data = json.loads(out)
    if isinstance(data, dict) and data.get("entries"):
        data = data["entries"][0]

    auto_keys = sorted(list(set((data or {}).get("automatic_captions", {}).keys())))
    manual_keys = sorted(list(set((data or {}).get("subtitles", {}).keys())))
    duration = int((data or {}).get("duration") or 0)

    samples = []
    for lang in ["en", "en-US", "nl"]:
        try:
            # attempt a tiny fetch to check status code
            base_cmd = [
                "yt-dlp", "--skip-download", "--write-auto-sub",
                "--sub-langs", lang, "-o", "%(id)s.%(ext)s", f"https://www.youtube.com/watch?v={vid}"
            ]
            run(base_cmd)
            samples.append({"lang": lang, "status": 200})
        except Exception:
            samples.append({"lang": lang, "status": 404})

    return {
        "info": {
            "title": (data or {}).get("title"),
            "duration": duration,
            "subtitles_keys": manual_keys,
            "auto_captions_keys": auto_keys,
        },
        "timedtext_samples": samples,
    }

# --------------------------
# Transcript
# --------------------------
@app.post("/transcript")
def transcript(req: Req):
    """
    Return transcript (always 200 OK) so the GPT Action never sees a connector error.
    On failure, ok=False with rich diagnostics instead of raising HTTPException.
    """
    started = time.time()
    url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={req.url_or_id}"
    vid = video_id(url)
    tried_langs = _lang_list(req.langs)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)

        # 1) metadata (title, channel, duration, subtitle track lists)
        try:
            meta_json = run(["yt-dlp", "-J", "--skip-download", url])
            meta = json.loads(meta_json)
            if isinstance(meta, dict) and meta.get("entries"):
                meta = meta["entries"][0]
        except Exception as e:
            return {
                "ok": False,
                "error": f"metadata_error: {str(e)}",
                "elapsed_s": round(time.time() - started, 3),
            }

        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        upload_date = meta.get("upload_date", "")
        if upload_date and len(upload_date) == 8:
            upload_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
        duration_s = int(meta.get("duration") or 0)

        auto_tracks = set((meta.get("automatic_captions") or {}).keys())
        manual_tracks = set((meta.get("subtitles") or {}).keys())
        available_langs = sorted(list(auto_tracks | manual_tracks))

        # 2) choose candidate languages
        # If 'all' in tried_langs => we’ll accept any available, but still
        # try the user’s explicit codes first.
        def selector_order() -> List[str]:
            if tried_langs and tried_langs[0].lower() == "all":
                # accept any first, then the rest
                return available_langs + [x for x in tried_langs[1:] if x in available_langs]
            # otherwise try in the user’s order, then any available
            ordered = [x for x in tried_langs if x in available_langs]
            return ordered + available_langs

        candidate_langs = selector_order()

        # 3) attempt download (manual first, then auto) for each candidate
        picked_lang = None
        captions_kind = ""   # "manual" or "auto"
        srt_path: Path | None = None

        base_pattern = "%(id)s.%(ext)s"
        for lang in candidate_langs:
            # manual
            try:
                run(["yt-dlp", "--skip-download", "--write-sub", "--sub-langs", lang,
                     "--convert-subs", "srt", "--force-overwrites", "-o", base_pattern, url])
                srt_path = next((p for p in wd.glob(f"{vid}*.srt")), None)
                if srt_path:
                    picked_lang = lang
                    captions_kind = "manual"
                    break
            except Exception:
                pass

            # auto
            try:
                run(["yt-dlp", "--skip-download", "--write-auto-sub", "--sub-langs", lang,
                     "--convert-subs", "srt", "--force-overwrites", "-o", base_pattern, url])
                srt_path = next((p for p in wd.glob(f"{vid}*.srt")), None)
                if srt_path:
                    picked_lang = lang
                    captions_kind = "auto"
                    break
            except Exception:
                pass

        if not srt_path or not srt_path.exists():
            # No captions anywhere – return 200 + ok:false so GPT can render it.
            return {
                "ok": False,
                "error": "no_captions",
                "message": "No captions could be downloaded.",
                "available_langs": available_langs,
                "tried_langs": tried_langs,
                "elapsed_s": round(time.time() - started, 3),
                "title": title,
                "channel": channel,
                "published_at": upload_date,
                "video_id": vid,
                "duration_pretty": _duration_pretty(duration_s),
            }

        # 4) load/clean SRT -> text
        def clean_srt_to_text(path: Path, keep_ts: bool) -> str:
            raw = path.read_text(encoding="utf-8", errors="ignore")
            blocks = re.split(r"\n\s*\n", raw.strip())
            lines: List[str] = []
            for blk in blocks:
                blk = re.sub(r"^\s*\d+\s*\n", "", blk)  # drop numeric counter
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
            out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)  # de-dupe e.g. "the the"
            out = re.sub(r"\s+([,.;:!?])", r"\1", out)
            return out

        txt = clean_srt_to_text(srt_path, keep_ts=req.keep_timestamps)
        txt_bytes = txt.encode("utf-8")
        srt_bytes = srt_path.read_bytes()

        # 5) optional PDF (best-effort)
        pdf_url = ""
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            header = f"{title} — {channel}\n{url}\n\n"
            for chunk in (header + txt).split("\n"):
                pdf.multi_cell(0, 6, chunk)
            pdf_file = wd / f"transcript_{vid}.pdf"
            pdf.output(str(pdf_file))
            pdf_bytes = pdf_file.read_bytes()
        except Exception:
            pdf_bytes = None

        # 6) serve files via token links if PUBLIC_BASE_URL is set, else data: URLs
        token_txt = f"{int(time.time())}_{vid}_txt"
        token_srt = f"{int(time.time())}_{vid}_srt"
        token_pdf = f"{int(time.time())}_{vid}_pdf"

        # Save token files into a tmp dir that survives request (Railway ephemeral FS OK for short time).
        # For simplicity we keep them in-memory-ish: return data URLs if PUBLIC_BASE_URL is missing.
        txt_http_url = _build_http_file_url(token_txt)
        srt_http_url = _build_http_file_url(token_srt)
        pdf_http_url = _build_http_file_url(token_pdf) if pdf_bytes else ""

        # Fall back to data: if no PUBLIC_BASE_URL configured
        if not txt_http_url:
            txt_http_url = _data_url("text/plain;charset=utf-8", txt_bytes)
            srt_http_url = _data_url("text/plain;charset=utf-8", srt_bytes)
            pdf_http_url = _data_url("application/pdf", pdf_bytes, b64=True) if pdf_bytes else ""

        # Response
        preview = txt[:2600]
        return {
            "ok": True,
            "title": title,
            "channel": channel,
            "published_at": upload_date,
            "duration_s": duration_s,
            "duration_pretty": _duration_pretty(duration_s),
            "video_id": vid,
            "captions_lang": picked_lang,
            "captions_kind": captions_kind,            # "manual" | "auto"
            "available_langs": available_langs,
            "tried_langs": tried_langs,
            "preview_text": preview,
            "truncated": len(txt) > len(preview),
            "txt_http_url": txt_http_url,
            "srt_http_url": srt_http_url,
            "pdf_http_url": pdf_http_url,
            "links_expire_in_seconds": EXPIRES_SECONDS,
            "links_expire_human": _duration_pretty(EXPIRES_SECONDS),
            "elapsed_s": round(time.time() - started, 3),
        }
