# main.py  —  stable baseline

import os, io, time, uuid, json, shutil, tempfile
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import yt_dlp

APP = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
EXPIRES_IN_SECONDS = int(os.getenv("EXPIRES_IN_SECONDS", "86400"))  # default 24h

# in-memory file registry: token -> {"path":..., "ctype":..., "expires_at":...}
FILES: Dict[str, Dict[str, Any]] = {}

# ---------- Models ----------
class TranscriptReq(BaseModel):
    url_or_id: str
    langs: str   # e.g., "en,en-US,en-GB,all" or "nl,all"
    keep_timestamps: bool = False


# ---------- Helpers ----------
def _ytdlp_info(url_or_id: str) -> Dict[str, Any]:
    ydl_opts = {"quiet": True, "skip_download": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(url_or_id, download=False)

def _collapse_langs(s: str) -> List[str]:
    # normalize, remove empties, lower
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out = []
    seen = set()
    for p in parts:
        p = p.strip()
        if p.lower() == "all":
            out.append("all")
        else:
            p = p  # keep case like en-US; comparisons use lower()
        if p not in seen:
            seen.add(p); out.append(p)
    if not out:
        out = ["en","en-US","en-GB","all"]
    return out

def _pick_caption_track(info: Dict[str, Any], langs: List[str]) -> Optional[Dict[str, Any]]:
    """
    Look through manual and auto captions in the order of 'langs'.
    Each track we return has keys: {"url":..., "ext": ("vtt"|"ttml"|"xml"), "lang":..., "kind":"manual|auto"}.
    """
    # Build catalog
    manual = info.get("subtitles") or {}
    auto   = info.get("automatic_captions") or {}

    def tracks_from(d: Dict[str, List[Dict[str, Any]]], kind: str):
        res = []
        for lang, items in d.items():
            # choose a webvtt first, else xml/ttml if present
            choice = None
            # try vtt
            for it in items:
                if (it.get("ext") or "").lower() in ("vtt","webvtt"):
                    choice = {"url": it["url"], "ext": "vtt", "lang": lang, "kind": kind}
                    break
            if not choice:
                for it in items:
                    ext = (it.get("ext") or "").lower()
                    if ext in ("ttml","xml"):
                        choice = {"url": it["url"], "ext": "xml", "lang": lang, "kind": kind}
                        break
            if choice:
                res.append(choice)
        return res

    manual_tracks = tracks_from(manual, "manual")
    auto_tracks   = tracks_from(auto, "auto")

    all_langs_available = {t["lang"] for t in manual_tracks} | {t["lang"] for t in auto_tracks}

    def find_for(code: str) -> Optional[Dict[str, Any]]:
        # try exact matches in manual, then auto; then case-insensitive fallbacks and startswith matches
        for pool in (manual_tracks, auto_tracks):
            for t in pool:
                if t["lang"] == code: return t
        lc = code.lower()
        for pool in (manual_tracks, auto_tracks):
            for t in pool:
                if t["lang"].lower() == lc: return t
        # startswith fallback (e.g., "en" finds "en-US")
        for pool in (manual_tracks, auto_tracks):
            for t in pool:
                if t["lang"].lower().startswith(lc): return t
        return None

    # user order
    for code in langs:
        if code.lower() == "all":
            # take a reasonable preference order
            for pref in ["en","en-US","en-GB","nl"]:
                t = find_for(pref)
                if t: return t
            # else: first available
            if manual_tracks: return manual_tracks[0]
            if auto_tracks: return auto_tracks[0]
            return None
        t = find_for(code)
        if t: return t

    # nothing found
    return None

def _download_caption(track: Dict[str, Any]) -> str:
    """
    Download caption URL into a temp file, return path to a .vtt or .srt.
    If xml/ttml, convert to .srt with yt_dlp helper.
    """
    url = track["url"]
    ext = track["ext"]  # "vtt" or "xml"
    tmp = tempfile.mkdtemp(prefix="caps_")
    raw_path = os.path.join(tmp, f"caption.{ext}")
    with yt_dlp.YoutubeDL({"quiet": True}) as ydl:
        # Use ydl.urlopen to fetch
        data = ydl.urlopen(url).read()
    with open(raw_path, "wb") as f:
        f.write(data)

    # normalize to .srt and .txt
    if ext == "vtt":
        vtt_path = raw_path
        srt_path = os.path.join(tmp, "caption.srt")
        # yt-dlp doesn't convert in-place; do a minimal vtt->srt conversion
        # Simple heuristic for stability: use webvtt-to-srt approach
        srt_text = _simple_vtt_to_srt(open(vtt_path, "r", encoding="utf-8", errors="ignore").read())
        with open(srt_path, "w", encoding="utf-8") as f:
            f.write(srt_text)
    else:
        # ext xml/ttml -> produce a naive text (strip tags) and srt-ish timestamps best-effort
        xml_txt = open(raw_path, "r", encoding="utf-8", errors="ignore").read()
        srt_path = os.path.join(tmp, "caption.srt")
        srt_text = _simple_xml_to_srt(xml_txt)
        with open(srt_path, "w", encoding="utf-8") as f:
            f.write(srt_text)

    # derive .txt
    txt_path = os.path.join(tmp, "caption.txt")
    with open(srt_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.read().splitlines()
    # remove indexes and timestamps
    cleaned = []
    import re
    ts_re = re.compile(r"\d{2}:\d{2}:\d{2},\d{3}\s-->\s\d{2}:\d{2}:\d{2},\d{3}")
    for line in lines:
        if not line.strip(): 
            continue
        if line.strip().isdigit(): 
            continue
        if ts_re.match(line): 
            continue
        cleaned.append(line)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(cleaned))

    return tmp  # directory holding caption.srt and caption.txt


def _simple_vtt_to_srt(vtt_text: str) -> str:
    # drop WEBVTT header; replace '.' ms with ','; keep cues
    lines = vtt_text.splitlines()
    out = []
    idx = 1
    import re
    tline = re.compile(r"(\d{2}:\d{2}:\d{2})\.(\d{3})\s-->\s(\d{2}:\d{2}:\d{2})\.(\d{3})")
    for line in lines:
        m = tline.search(line)
        if m:
            out.append(str(idx))
            out.append(f"{m.group(1)},{m.group(2)} --> {m.group(3)},{m.group(4)}")
            idx += 1
        elif line.strip().upper().startswith("WEBVTT"):
            continue
        else:
            out.append(line)
    return "\n".join(out)

def _simple_xml_to_srt(xml_text: str) -> str:
    # minimal TTML/DFXP parser (best-effort)
    import re, html
    # find <p begin="..." end="...">text</p>
    p_re = re.compile(r'<p[^>]*begin="([^"]+)"[^>]*end="([^"]+)"[^>]*>(.*?)</p>', re.S)
    def to_srt_ts(t):
        # 00:00:01.234 or 1.234s
        if t.endswith("s"):
            seconds = float(t[:-1])
        else:
            parts = t.split(":")
            if len(parts) == 3:
                h,m,s = parts
                seconds = int(h)*3600 + int(m)*60 + float(s)
            else:
                seconds = float(t)
        ms = int(round((seconds - int(seconds)) * 1000))
        seconds = int(seconds)
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"
    out = []
    idx = 1
    for m in p_re.finditer(xml_text):
        out.append(str(idx))
        out.append(f"{to_srt_ts(m.group(1))} --> {to_srt_ts(m.group(2))}")
        out.append(html.unescape(re.sub("<.*?>","",m.group(3))).strip())
        out.append("")
        idx += 1
    return "\n".join(out) if out else ""

def _register_file(path: str, ctype: str) -> str:
    token = uuid.uuid4().hex
    FILES[token] = {"path": path, "ctype": ctype, "expires_at": time.time() + EXPIRES_IN_SECONDS}
    return token

def _purge_expired():
    now = time.time()
    to_del = [t for t,meta in FILES.items() if meta["expires_at"] <= now]
    for t in to_del:
        try:
            base = os.path.dirname(FILES[t]["path"])
            shutil.rmtree(base, ignore_errors=True)
        except:
            pass
        FILES.pop(t, None)


# ---------- Routes ----------
@APP.get("/health")
def health():
    _purge_expired()
    return {"ok": True, "egress_to_youtube": True, "expires_in_seconds_default": EXPIRES_IN_SECONDS}

@APP.get("/probe")
def probe(url: str):
    info = _ytdlp_info(url)
    manual = sorted(list((info.get("subtitles") or {}).keys()))
    auto   = sorted(list((info.get("automatic_captions") or {}).keys()))
    # quick timedtext samples (en, en-US, nl) to see status
    samples = []
    for lang in ["en","en-US","nl"]:
        st = 404
        try:
            tr = _pick_caption_track(info, [lang])
            st = 200 if tr else 404
        except:
            st = 404
        samples.append({"lang": lang, "status": st})
    return {"info": {"title": info.get("title"), "duration": info.get("duration")},
            "subtitles_keys": manual, "auto_captions_keys": auto,
            "timedtext_samples": samples}

@APP.get("/file/{token}")
def get_file(token: str = Path(...)):
    meta = FILES.get(token)
    if not meta:
        raise HTTPException(status_code=404, detail="Expired or invalid token.")
    if meta["expires_at"] <= time.time():
        _purge_expired()
        raise HTTPException(status_code=404, detail="Expired token.")
    return FileResponse(meta["path"], media_type=meta["ctype"])

@APP.post("/transcript")
def transcript(req: TranscriptReq):
    _purge_expired()

    info = _ytdlp_info(req.url_or_id)
    langs = _collapse_langs(req.langs)
    track = _pick_caption_track(info, langs)
    if not track:
        # surface available languages to caller
        manual = sorted(list((info.get("subtitles") or {}).keys()))
        auto   = sorted(list((info.get("automatic_captions") or {}).keys()))
        raise HTTPException(
            status_code=404,
            detail=f"No captions were found (manual/auto). Available tracks (non-empty): manual={manual}, auto={auto}. "
                   f"Try passing langs='all' or a specific code you see listed (e.g., 'nl')."
        )

    tmpdir = _download_caption(track)
    srt_path = os.path.join(tmpdir, "caption.srt")
    txt_path = os.path.join(tmpdir, "caption.txt")

    # Build preview
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        full_txt = f.read()
    preview = full_txt[:2500]
    truncated = len(full_txt) > len(preview)

    # register files
    txt_token = _register_file(txt_path, "text/plain")
    srt_token = _register_file(srt_path, "application/x-subrip")

    pdf_url = ""  # (optional) you can add a PDF generator later
    title = info.get("title") or ""
    channel = (info.get("uploader") or info.get("channel")) or ""
    published = info.get("upload_date")
    if published and len(published) == 8:
        published = f"{published[0:4]}-{published[4:6]}-{published[6:8]}"

    duration = info.get("duration") or 0
    duration_pretty = f"{duration//60}m {duration%60:02d}s" if duration else ""

    base = PUBLIC_BASE_URL or ""  # if empty, URLs will be relative (not recommended)
    txt_http_url = f"{base}/file/{txt_token}" if base else f"/file/{txt_token}"
    srt_http_url = f"{base}/file/{srt_token}" if base else f"/file/{srt_token}"

    return {
        "title": title,
        "channel": channel,
        "published_at": published,
        "duration_s": duration,
        "duration_pretty": duration_pretty,
        "video_id": info.get("id"),
        "captions_lang": track["lang"],
        "captions_kind": track["kind"],
        "preview_text": preview,
        "truncated": truncated,
        "txt_http_url": txt_http_url,
        "srt_http_url": srt_http_url,
        "pdf_http_url": pdf_url,
        "links_expire_in_seconds": EXPIRES_IN_SECONDS,
        "links_expire_human": f"{EXPIRES_IN_SECONDS//3600}h" if EXPIRES_IN_SECONDS>=3600 else f"{EXPIRES_IN_SECONDS//60}m",
    }


# ---------- Run (for local) ----------
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(APP, host="0.0.0.0", port=int(os.getenv("PORT","8080")))
