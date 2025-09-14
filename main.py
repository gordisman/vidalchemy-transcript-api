import re
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import JSONResponse
import yt_dlp

app = FastAPI(title="Creator Transcript Fetcher", version="2.2.0")


# ---------- Helpers ----------

def _extract_info(url: str) -> Dict:
    """Call yt-dlp for metadata only (no download)."""
    ydl_opts = {
        "skip_download": True,
        "quiet": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(url, download=False)


def _available_tracks(info: Dict) -> Tuple[List[str], List[str]]:
    """
    Return (manual_langs, auto_langs) from yt-dlp info.
    Keys are language codes (e.g., 'en', 'en-US', 'nl').
    """
    subs = info.get("subtitles") or {}
    autos = info.get("automatic_captions") or {}
    manual_langs = sorted(list(subs.keys()))
    auto_langs = sorted(list(autos.keys()))
    return manual_langs, auto_langs


def _pick_language(
    requested: List[str],
    manual_langs: List[str],
    auto_langs: List[str],
) -> Tuple[str, str]:
    """
    Choose the best language according to the requested list.
    Returns (lang, kind) where kind in {'manual','auto'}.
    Rules:
      1) For each requested code X (except 'all'):
           - pick manual X if present, else auto X if present
      2) If 'all' is present:
           - prefer any manual (first), else any auto (first)
      3) If nothing matches, raise.
    """
    manual_set = set(manual_langs)
    auto_set = set(auto_langs)

    for code in requested:
        if code == "all":
            break
        # exact code match (manual preferred)
        if code in manual_set:
            return code, "manual"
        if code in auto_set:
            return code, "auto"

        # loose fallback: if code is 'en', try any 'en-*'
        if code in {"en", "nl", "fr", "de", "es", "pt"}:
            manual_family = [l for l in manual_langs if l == code or l.startswith(f"{code}-")]
            auto_family = [l for l in auto_langs if l == code or l.startswith(f"{code}-")]
            if manual_family:
                return manual_family[0], "manual"
            if auto_family:
                return auto_family[0], "auto"

    # 'all' means "take anything" (manual first)
    if "all" in requested:
        if manual_langs:
            return manual_langs[0], "manual"
        if auto_langs:
            return auto_langs[0], "auto"

    raise HTTPException(
        status_code=404,
        detail=f"No captions match requested langs={requested}. "
               f"Available manual={manual_langs}, auto={auto_langs}"
    )


def _strip_srt_timestamps(srt_text: str) -> str:
    """
    Convert SRT to plain text by removing index lines and time ranges.
    """
    # Remove blocks like:
    # 12
    # 00:00:01,200 --> 00:00:03,400
    # text...
    out = re.sub(r"(?m)^\d+\s*$", "", srt_text)  # index lines
    out = re.sub(r"(?m)^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}.*$", "", out)
    # Collapse multiple blank lines
    out = re.sub(r"\n{2,}", "\n\n", out).strip()
    return out


# ---------- Endpoints ----------

@app.get("/health")
def health():
    return {
        "ok": True,
        "egress_to_youtube": True,
        "expires_in_seconds_default": 86400,
    }


@app.get("/probe")
def probe(url: str):
    """
    Inspect the video without downloading captions; useful to see what languages exist.
    """
    info = _extract_info(url)
    manual_langs, auto_langs = _available_tracks(info)
    # quick sample check for common codes
    samples = []
    for code in ["en", "en-US", "nl"]:
        status = 200 if (code in manual_langs or code in auto_langs) else 404
        samples.append({"lang": code, "status": status})
    return {
        "info": {
            "title": info.get("title"),
            "duration": info.get("duration"),
        },
        "subtitles_keys": manual_langs,
        "auto_captions_keys": auto_langs,
        "timedtext_samples": samples,
    }


@app.post("/transcript")
def transcript(payload: Dict = Body(...)):
    """
    Fetch captions for a video in the best-matching language.
    Request body:
      {
        "url_or_id": "<url-or-youtube-id>",
        "langs": "en,en-US,en-GB,all",  # optional, comma-separated
        "keep_timestamps": false        # optional
      }
    Response:
      {
        "title": "...",
        "duration": 1234,
        "captions_lang": "en",
        "captions_kind": "auto" | "manual",
        "txt_content": "...",   # plain text
        "srt_content": "..."    # original SRT
      }
    """
    url_or_id = payload.get("url_or_id")
    if not url_or_id:
        raise HTTPException(status_code=422, detail="Missing 'url_or_id'.")

    langs_str = payload.get("langs") or "en,en-US,en-GB,all"
    requested = [x.strip() for x in langs_str.split(",") if x.strip()]
    keep_ts = bool(payload.get("keep_timestamps", False))

    # 1) Probe video & pick language
    info = _extract_info(url_or_id)
    manual_langs, auto_langs = _available_tracks(info)

    if not manual_langs and not auto_langs:
        raise HTTPException(status_code=404, detail="No captions were found (manual/auto).")

    chosen_lang, chosen_kind = _pick_language(requested, manual_langs, auto_langs)

    # 2) Download that language as SRT
    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)

        ydl_opts = {
            "skip_download": True,
            "quiet": True,
            "writesubtitles": True,        # manual
            "writeautomaticsub": True,     # auto
            "subtitleslangs": [chosen_lang],
            "subtitlesformat": "srt",
            "outtmpl": str(tmpdir / "%(id)s.%(ext)s"),
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            _ = ydl.extract_info(url_or_id, download=False)

        # Find the produced .srt
        srt_file = None
        for p in tmpdir.glob("*.srt"):
            srt_file = p
            break
        if not srt_file or not srt_file.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Captions for lang='{chosen_lang}' could not be downloaded."
            )

        srt_text = srt_file.read_text(encoding="utf-8", errors="ignore")
        if keep_ts:
            txt_text = srt_text
        else:
            txt_text = _strip_srt_timestamps(srt_text)

    # 3) Return inline
    return JSONResponse(
        {
            "title": info.get("title"),
            "duration": info.get("duration"),
            "captions_lang": chosen_lang,
            "captions_kind": chosen_kind,  # "manual" or "auto"
            "txt_content": txt_text,
            "srt_content": srt_text,
        }
    )
