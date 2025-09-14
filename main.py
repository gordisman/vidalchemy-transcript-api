# main.py
import os, re, json, base64, tempfile, subprocess, time, uuid, shutil
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel

# ----------------------------
# Config
# ----------------------------
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
EXPIRES_IN_SECONDS = int(os.environ.get("EXPIRES_IN_SECONDS", "86400"))  # 24h default

if not PUBLIC_BASE_URL:
    raise RuntimeError("PUBLIC_BASE_URL environment variable is required")

# ----------------------------
# App & CORS
# ----------------------------
app = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

# ----------------------------
# Simple in-memory file token store
# ----------------------------
# token -> {"path": Path, "mime": str, "filename": str, "expires": float}
FILE_TOKENS: Dict[str, Dict[str, Any]] = {}

def _now() -> float:
    return time.time()

def _pretty_duration(seconds: int) -> str:
    if seconds <= 0:
        return "0s"
    h, m = divmod(seconds, 3600)
    m, s = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def _cleanup_tokens() -> None:
    """Remove expired tokens & orphan files."""
    now = _now()
    dead: List[str] = []
    for tok, meta in FILE_TOKENS.items():
        if meta["expires"] <= now or not Path(meta["path"]).exists():
            dead.append(tok)
    for tok in dead:
        try:
            p = Path(FILE_TOKENS[tok]["path"])
            if p.exists():
                p.unlink(missing_ok=True)
        finally:
            FILE_TOKENS.pop(tok, None)

def _make_download_token(path: Path, mime: str, filename: str) -> str:
    token = uuid.uuid4().hex
    FILE_TOKENS[token] = {
        "path": str(path),
        "mime": mime,
        "filename": filename,
        "expires": _now() + EXPIRES_IN_SECONDS,
    }
    return token

def _token_url(token: str) -> str:
    return f"{PUBLIC_BASE_URL}/file/{token}"

# ----------------------------
# Models
# ----------------------------
class Req(BaseModel):
    url_or_id: str
    langs: str = "en,en-US,en-GB,all"
    keep_timestamps: bool = False

# ----------------------------
# Helpers
# ----------------------------
YT_ID_RE = re.compile(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})")

def _video_id(u: str) -> str:
    m = YT_ID_RE.search(u)
    return m.group(1) if m else u.strip()

def _run(cmd: List[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr or p.stdout)
    return p.stdout

def _get_meta(url: str) -> Dict[str, Any]:
    out = _run(["yt-dlp", "-J", "--skip-download", url])
    data = json.loads(out)
    if isinstance(data, dict) and data.get("entries"):
        data = data["entries"][0]
    return data if isinstance(data, dict) else {}

def _pick_langs(langs: str) -> str:
    """Normalize 'all' usage for yt-dlp."""
    langs = (langs or "").strip()
    if not langs:
        return "en,en-US,en-GB,all"
    # If the user included all, keep it as 'all' (yt-dlp understands it)
    parts = [p.strip() for p in langs.split(",") if p.strip()]
    # de-dup while preserving order
    seen, out = set(), []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return ",".join(out)

SRT_BLOCK_SPLIT = re.compile(r"\n\s*\n")

def _clean_srt_to_text(srt_path: Path, keep_ts: bool) -> str:
    raw = srt_path.read_text(encoding="utf-8", errors="ignore")
    blocks = SRT_BLOCK_SPLIT.split(raw.strip())
    lines: List[str] = []
    for blk in blocks:
        # drop numeric index lines
        blk = re.sub(r"^\s*\d+\s*\n", "", blk)
        # capture the timestamp line if needed
        ts_match = re.search(r"^(\d{2}:\d{2}:\d{2}),\d{3}\s*-->", blk, flags=re.M)
        # remove timestamp lines
        text = re.sub(
            r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$",
            "",
            blk,
        )
        # collapse whitespace and join
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if keep_ts and ts_match:
            lines.append(f"{ts_match.group(1)} {text}")
        else:
            lines.append(text)
    out = "\n".join(lines) if keep_ts else " ".join(lines)
    # remove duplicate word runs and fix spacing before punct
    out = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    return out

def _write_pdf(title: str, channel: str, url: str, text: str, out_path: Path) -> bool:
    try:
        from fpdf import FPDF
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        header = f"{title} — {channel}\n{url}\n\n"
        for line in (header + text).split("\n"):
            pdf.multi_cell(0, 6, line)
        pdf.output(str(out_path))
        return True
    except Exception:
        return False

def _first_existing(paths: List[Path]) -> Optional[Path]:
    for p in paths:
        if p and p.exists():
            return p
    return None

# ----------------------------
# Probe endpoint (debug aid)
# ----------------------------
@app.get("/probe")
def probe(url: str):
    """
    Quick view of what YouTube reports:
    - subtitles_keys (manual)
    - auto_captions_keys (auto)
    - some timedtext sample status codes
    """
    meta = _get_meta(url)
    subtitles = list((meta.get("subtitles") or {}).keys())
    auto_caps = list((meta.get("automatic_captions") or {}).keys())

    # Try a couple of common langs to see if reachable
    samples = []
    for lang in ["en", "en-US", "nl"]:
        try:
            # yt-dlp test: just check if status 200 possible via --print?
            # we'll rely on presence in automatic_captions/subtitles instead.
            samples.append({"lang": lang, "status": 200 if (lang in auto_caps or lang in subtitles) else 404})
        except Exception:
            samples.append({"lang": lang, "status": 500})

    info = {
        "title": meta.get("title"),
        "duration": int(meta.get("duration") or 0),
        "subtitles_keys": subtitles,
        "auto_captions_keys": auto_caps,
    }
    return {"info": info, "timedtext_samples": samples}

# ----------------------------
# Health endpoint
# ----------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "egress_to_youtube": True,
        "expires_in_seconds_default": EXPIRES_IN_SECONDS,
    }

# ----------------------------
# Download endpoint
# ----------------------------
@app.get("/file/{token}")
def get_file(token: str):
    _cleanup_tokens()
    meta = FILE_TOKENS.get(token)
    if not meta:
        raise HTTPException(status_code=404, detail="Link expired or invalid.")
    path = Path(meta["path"])
    if not path.exists():
        FILE_TOKENS.pop(token, None)
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(
        path,
        media_type=meta["mime"],
        filename=meta["filename"],
    )

# ----------------------------
# /transcript: main handler
# ----------------------------
@app.post("/transcript")
def transcript(req: Req):
    url = req.url_or_id if req.url_or_id.startswith("http") else f"https://www.youtube.com/watch?v={req.url_or_id}"
    vid = _video_id(url)
    langs = _pick_langs(req.langs)

    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)

        # ---------- Metadata ----------
        meta = _get_meta(url)
        title = meta.get("title", "")
        channel = meta.get("channel") or meta.get("uploader", "")
        up = meta.get("upload_date", "")
        published_at = f"{up[:4]}-{up[4:6]}-{up[6:]}" if up and len(up) == 8 else ""
        duration_s = int(meta.get("duration") or 0)
        duration_pretty = _pretty_duration(duration_s)

        subtitles_keys = list((meta.get("subtitles") or {}).keys())
        auto_keys = list((meta.get("automatic_captions") or {}).keys())

        # Build base yt-dlp args
        out_tpl = "%(id)s.%(ext)s"
        base = [
            "yt-dlp",
            "--skip-download",
            "--convert-subs", "srt",
            "--force-overwrites",
            "-o", out_tpl,
            url,
        ]

        def try_fetch(mode: str, sub_langs: str) -> Tuple[Optional[Path], Optional[str], str]:
            """
            mode: "--write-sub" or "--write-auto-sub"
            Returns (srt_path, picked_lang, kind)
            """
            try:
                args = ["yt-dlp", "--skip-download", mode, "--sub-langs", sub_langs,
                        "--convert-subs", "srt", "--force-overwrites", "-o", out_tpl, url]
                _run(args)
            except Exception:
                pass

            # Find srt/vtt created for this video id
            # yt-dlp names subs like: <id>.<lang>.srt
            # We'll try to pick a reasonable best match
            created = list(wd.glob(f"{vid}*.srt"))
            if not created:
                # maybe vtt if conversion failed
                created_vtt = list(wd.glob(f"{vid}*.vtt"))
                if created_vtt:
                    # convert first one to srt
                    vtt = created_vtt[0]
                    srt_out = vtt.with_suffix(".srt")
                    try:
                        _run(["ffmpeg", "-y", "-i", str(vtt), str(srt_out)])
                        created = [srt_out]
                    except Exception:
                        created = []

            if not created:
                return None, None, "none"

            # Choose one (prefer en/en-US/en-GB if present)
            def lang_from_name(p: Path) -> str:
                # name: <id>.<lang>.srt
                m = re.match(rf"^{re.escape(vid)}\.(.+?)\.srt$", p.name)
                return m.group(1) if m else ""

            ranks = {"en": 0, "en-US": 0, "en-GB": 0}
            created_sorted = sorted(created, key=lambda p: ranks.get(lang_from_name(p), 1))
            best = created_sorted[0]
            return best, lang_from_name(best), "manual" if mode == "--write-sub" else "auto"

        # 1) Try MANUAL first if YouTube says any exist
        picked_srt = None
        picked_lang = None
        captions_kind = "none"

        if subtitles_keys:
            srt, lang, kind = try_fetch("--write-sub", langs)
            if srt:
                picked_srt, picked_lang, captions_kind = srt, lang, kind

        # 2) If still nothing, try AUTO
        if not picked_srt:
            srt, lang, kind = try_fetch("--write-auto-sub", langs)
            if srt:
                picked_srt, picked_lang, captions_kind = srt, lang, kind

        # 3) Still nothing → 404
        if not picked_srt:
            # Be helpful: tell the caller what *was* reported
            detail = "No captions were found (manual/auto). Try a different video or pass a specific language (e.g., 'nl')."
            if subtitles_keys or auto_keys:
                detail = (
                    "No captions were found (manual/auto). "
                    f"Available tracks reported were manual={subtitles_keys}, auto={auto_keys}. "
                    "Try passing a specific language you see listed."
                )
            raise HTTPException(status_code=404, detail=detail)

        # ---------- Build artifacts ----------
        txt_text = _clean_srt_to_text(picked_srt, keep_ts=req.keep_timestamps)
        txt_path = wd / f"{vid}.txt"
        txt_path.write_text(txt_text, encoding="utf-8")

        pdf_path = wd / f"{vid}.pdf"
        pdf_ok = _write_pdf(title, channel, url, txt_text, pdf_path)

        # ---------- Make download tokens ----------
        token_txt = _make_download_token(txt_path, "text/plain; charset=utf-8", f"{vid}_{picked_lang}.txt")
        token_srt = _make_download_token(picked_srt, "text/plain; charset=utf-8", f"{vid}_{picked_lang}.srt")
        pdf_url = ""
        if pdf_ok and pdf_path.exists():
            token_pdf = _make_download_token(pdf_path, "application/pdf", f"{vid}_{picked_lang}.pdf")
            pdf_url = _token_url(token_pdf)

        preview = txt_text[:2500]
        return {
            "title": title,
            "channel": channel,
            "published_at": published_at,
            "duration_s": duration_s,
            "duration_pretty": duration_pretty,
            "video_id": vid,
            "preview_text": preview,
            "truncated": len(txt_text) > len(preview),
            "captions_lang": picked_lang or "",
            "captions_kind": captions_kind,  # "manual" or "auto"
            "txt_http_url": _token_url(token_txt),
            "srt_http_url": _token_url(token_srt),
            "pdf_http_url": pdf_url,
            "links_expire_in_seconds": EXPIRES_IN_SECONDS,
        }
