#!/usr/bin/env python3
"""ClipForge worker: CPU-first, offline ASR, async queue, HMAC callbacks."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import queue
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.request
import uuid
import wave
import sys
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, HttpUrl

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", APP_DIR / "data"))
MODEL_DIR = Path(os.environ.get("MODEL_DIR", APP_DIR / "models" / "vosk-model-small-pt-0.3"))
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "http://localhost:7860").rstrip("/")
SECRET = os.environ.get("WORKER_WEBHOOK_SECRET", "")
MAX_DOWNLOAD = int(os.environ.get("MAX_DOWNLOAD_MB", "1500")) * 1024 * 1024
CALLBACK_ENABLED = os.environ.get("CALLBACK_ENABLED", "true").lower() == "true"
DEFER_READY_CALLBACK = os.environ.get("DEFER_READY_CALLBACK", "false").lower() == "true"
PUBLIC_FILE_PREFIX = os.environ.get("PUBLIC_FILE_PREFIX", "")
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "")
YOUTUBE_COOKIES_B64 = os.environ.get("YOUTUBE_COOKIES_B64", "")
VOSK_PYTHON_PATH = os.environ.get("VOSK_PYTHON_PATH", "")
if VOSK_PYTHON_PATH:
    sys.path.insert(0, VOSK_PYTHON_PATH)
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "jobs.sqlite3"

app = FastAPI(title="ClipForge Worker", version="0.1.0")
work_queue: queue.Queue[str] = queue.Queue()
model_lock = threading.Lock()
_vosk_model = None


class Source(BaseModel):
    type: Literal["upload", "youtube", "public_url"]
    url: HttpUrl


class JobRequest(BaseModel):
    job_id: str = Field(min_length=5, max_length=160)
    project_id: str = Field(min_length=5, max_length=160)
    workspace_id: str = Field(min_length=5, max_length=160)
    source: Source
    language: str = "pt-BR"
    prompt: str = ""
    settings: dict[str, Any] = Field(default_factory=dict)
    callback_url: HttpUrl


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, project_id TEXT NOT NULL, request_json TEXT NOT NULL,
            status TEXT NOT NULL, progress INTEGER NOT NULL DEFAULT 0,
            error TEXT NOT NULL DEFAULT '', result_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )""")
        conn.execute("UPDATE jobs SET status='queued', updated_at=? WHERE status='running'", (now(),))
        for row in conn.execute("SELECT id FROM jobs WHERE status='queued' ORDER BY created_at"):
            work_queue.put(row["id"])


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def update_job(job_id: str, status: str, progress: int, *, error: str = "", result: dict | None = None) -> None:
    with db() as conn:
        conn.execute("UPDATE jobs SET status=?, progress=?, error=?, result_json=?, updated_at=? WHERE id=?",
                     (status, max(0, min(100, progress)), error, json.dumps(result or {}), now(), job_id))


def job_row(job_id: str) -> dict[str, Any] | None:
    with db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        return None
    out = dict(row)
    out["request"] = json.loads(out.pop("request_json"))
    out["result"] = json.loads(out.pop("result_json"))
    return out


def sign_outbound(raw: bytes) -> str:
    return hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()


def verify_inbound(raw: bytes, signature: str) -> bool:
    if not SECRET or not signature:
        return False
    return hmac.compare_digest(sign_outbound(raw), signature)


def callback(req: dict, event_type: str, progress: int, data: dict | None = None) -> None:
    if not CALLBACK_ENABLED:
        return
    occurred = now()
    event_id = "evt_" + uuid.uuid4().hex
    body = {"event_id": event_id, "event_type": event_type, "job_id": req["job_id"],
            "project_id": req["project_id"], "occurred_at": occurred,
            "progress": progress, "data": data or {}}
    signature_base = f"{event_id}.{event_type}.{req['job_id']}.{occurred}".encode()
    signature = hmac.new(SECRET.encode(), signature_base, hashlib.sha256).hexdigest()
    delays = [0, 10, 30, 120, 600]
    for delay in delays:
        if delay:
            time.sleep(delay)
        try:
            response = httpx.post(req["callback_url"], json=body,
                                  headers={"X-ClipForge-Signature": signature}, timeout=20)
            if 200 <= response.status_code < 300:
                return
            if 400 <= response.status_code < 500 and response.status_code not in (408, 429):
                return
        except Exception:
            continue


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def probe(video: Path) -> dict:
    out = subprocess.check_output(["ffprobe", "-v", "error", "-show_entries",
                                   "format=duration:stream=codec_type,width,height,codec_name", "-of", "json", str(video)])
    return json.loads(out)


def r2_client():
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET]):
        return None
    import boto3
    return boto3.client("s3", endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
                        aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                        region_name="auto")


def download(url: str, dest: Path, source_type: str = "public_url") -> None:
    if source_type == "youtube":
        cookie_args = []
        cookie_path = dest.parent / "youtube-cookies.txt"
        if YOUTUBE_COOKIES_B64:
            cookie_path.write_bytes(base64.b64decode(YOUTUBE_COOKIES_B64))
            os.chmod(cookie_path, 0o600)
            cookie_args = ["--cookies", str(cookie_path)]
        run(["yt-dlp", "--no-playlist", *cookie_args, "--extractor-args", "youtube:player_client=android,web_safari", "--max-filesize", f"{MAX_DOWNLOAD // (1024 * 1024)}M",
             "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]", "--merge-output-format", "mp4",
             "-o", str(dest), url])
        if not dest.exists():
            raise ValueError("youtube download did not produce a video")
        return
    request = urllib.request.Request(url, headers={"User-Agent": "ClipForge/0.1"})
    with urllib.request.urlopen(request, timeout=60) as response, open(dest, "wb") as f:
        total = 0
        while chunk := response.read(1024 * 1024):
            total += len(chunk)
            if total > MAX_DOWNLOAD:
                raise ValueError("video exceeds MAX_DOWNLOAD_MB")
            f.write(chunk)


def get_model():
    global _vosk_model
    with model_lock:
        if _vosk_model is None:
            import vosk
            if not MODEL_DIR.exists():
                raise RuntimeError(f"Vosk model missing: {MODEL_DIR}")
            _vosk_model = vosk.Model(str(MODEL_DIR))
    return _vosk_model


def transcribe(video: Path, work: Path) -> list[dict]:
    import vosk
    metadata = probe(video)
    if not any(stream.get("codec_type") == "audio" for stream in metadata.get("streams", [])):
        raise ValueError("video has no audio stream")
    wav_path = work / "audio.wav"
    run(["ffmpeg", "-v", "error", "-y", "-i", str(video), "-ar", "16000", "-ac", "1", "-f", "wav", str(wav_path)])
    words: list[dict] = []
    with wave.open(str(wav_path), "rb") as wf:
        rec = vosk.KaldiRecognizer(get_model(), wf.getframerate())
        rec.SetWords(True)
        while data := wf.readframes(4000):
            if rec.AcceptWaveform(data):
                words.extend(_result_words(json.loads(rec.Result())))
        words.extend(_result_words(json.loads(rec.FinalResult())))
    return words


def _result_words(result: dict) -> list[dict]:
    return [{"w": x["word"], "start": round(x["start"], 2), "end": round(x["end"], 2),
             "confidence": round(x.get("conf", 0), 3)} for x in result.get("result", [])]


PT_HOOK = re.compile(r"\b(erro|segredo|nunca|maior|como|por que|verdade|mudar|futuro|problema|atenção|importante|primeiro)\b", re.I)


def candidates(words: list[dict], min_sec: int = 15, max_sec: int = 60) -> list[dict]:
    if not words:
        return []
    groups, current = [], [words[0]]
    for word in words[1:]:
        if word["start"] - current[-1]["end"] > 1.1 or word["end"] - current[0]["start"] > max_sec:
            groups.append(current); current = [word]
        else:
            current.append(word)
    groups.append(current)
    output = []
    for group in groups:
        duration = group[-1]["end"] - group[0]["start"]
        if duration < min_sec:
            continue
        text = " ".join(w["w"] for w in group)
        hook = 85 if PT_HOOK.search(" ".join(text.split()[:12])) else 60
        coherence = min(92, 58 + len(group) // 3)
        clarity = round(sum(w.get("confidence", 0) for w in group) / len(group) * 100)
        value = 72 if len(group) >= 25 else 60
        score = round(hook * .25 + coherence * .25 + value * .20 + clarity * .15 + 70 * .15)
        output.append({"start": group[0]["start"], "end": group[-1]["end"], "duration": round(duration, 2),
                       "score": score, "title": " ".join(text.split()[:8]).capitalize(), "text": text,
                       "words": group, "score_details": {"hook": hook, "coherence": coherence,
                       "value": value, "clarity": clarity, "prompt_fit": 70}})
    return sorted(output, key=lambda x: x["score"], reverse=True)[:10]


def make_ass(words: list[dict], start: float, path: Path) -> None:
    header = """[Script Info]\nScriptType: v4.00+\nPlayResX: 720\nPlayResY: 1280\nWrapStyle: 2\n\n[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\nStyle: Kinetic,Liberation Sans,72,&H00FFFFFF,&H000000FF,&H00000000,&H96000000,-1,0,0,0,100,100,1,0,1,5,2,5,40,40,260,1\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"""
    def ts(value: float) -> str:
        value = max(0, value - start); return f"0:{int(value//60):02d}:{value%60:05.2f}"
    lines = [header]
    for i, word in enumerate(words):
        end = words[i + 1]["start"] - .02 if i + 1 < len(words) else word["end"] + .25
        text = str(word["w"]).upper().replace("{", "").replace("}", "")
        lines.append(f"Dialogue: 0,{ts(word['start'])},{ts(end)},Kinetic,,0,0,0,,{{\\fad(60,30)\\fscx82\\fscy82\\t(0,100,\\fscx100\\fscy100)}}{text}\n")
    path.write_text("".join(lines), encoding="utf-8")


def render_preview(video: Path, clip: dict, out: Path) -> None:
    ass = out.with_suffix(".ass")
    make_ass(clip["words"], clip["start"], ass)
    duration = clip["end"] - clip["start"]
    vf = f"scale=720:1280:force_original_aspect_ratio=increase,crop=720:1280,subtitles='{ass}'"
    run(["ffmpeg", "-v", "error", "-y", "-ss", str(clip["start"]), "-t", str(duration),
         "-i", str(video), "-vf", vf, "-c:v", "libx264", "-preset", "veryfast", "-crf", "27",
         "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", str(out)])


def process(job_id: str) -> None:
    row = job_row(job_id)
    if not row:
        return
    req = row["request"]
    work = DATA_DIR / job_id
    work.mkdir(parents=True, exist_ok=True)
    video = work / "source.mp4"
    try:
        update_job(job_id, "running", 2); callback(req, "job.ingesting", 2)
        download(req["source"]["url"], video, req["source"].get("type", "public_url"))
        metadata = probe(video)
        update_job(job_id, "running", 18); callback(req, "job.transcribing", 18, {"duration_seconds": float(metadata["format"]["duration"])})
        words = transcribe(video, work)
        (work / "transcript.json").write_text(json.dumps({"words": words}, ensure_ascii=False), encoding="utf-8")
        update_job(job_id, "running", 55); callback(req, "job.analyzing", 55, {"word_count": len(words)})
        update_job(job_id, "running", 68); callback(req, "job.curating", 68)
        clips = candidates(words)
        (work / "clips.json").write_text(json.dumps({"clips": clips}, ensure_ascii=False), encoding="utf-8")
        update_job(job_id, "running", 80); callback(req, "job.previewing", 80, {"candidate_count": len(clips)})
        previews = []
        storage = r2_client()
        for idx, clip in enumerate(clips[:3], 1):
            out = work / f"preview_{idx}.mp4"
            render_preview(video, clip, out)
            storage_key = f"workspaces/{req['workspace_id']}/projects/{req['project_id']}/previews/{job_id}-{idx}.mp4"
            if storage:
                storage.upload_file(str(out), R2_BUCKET, storage_key, ExtraArgs={"ContentType": "video/mp4"})
                file_url = storage.generate_presigned_url("get_object", Params={"Bucket": R2_BUCKET, "Key": storage_key}, ExpiresIn=604800)
            else:
                file_url = (PUBLIC_FILE_PREFIX.replace("{job_id}", job_id).rstrip("/") + "/" + out.name) if PUBLIC_FILE_PREFIX else f"{PUBLIC_BASE_URL}/files/{job_id}/{out.name}"
                storage_key = ""
            previews.append({"rank": idx, "score": clip["score"], "title": clip["title"],
                             "url": file_url, "storage_key": storage_key})
        result = {"metadata": metadata, "word_count": len(words), "candidates": clips, "previews": previews}
        update_job(job_id, "completed", 100, result=result)
        if not DEFER_READY_CALLBACK:
            callback(req, "project.ready", 100, result)
    except Exception as exc:
        message = str(exc)
        if "yt-dlp" in message:
            message = "O YouTube bloqueou o download automatizado deste vídeo. Use um link MP4 público direto ou tente outro vídeo público."
        elif "no audio stream" in message:
            message = "O vídeo não possui faixa de áudio para transcrição."
        update_job(job_id, "failed", row.get("progress", 0), error=message)
        callback(req, "job.failed", row.get("progress", 0), {"error": message[:500]})


def worker_loop() -> None:
    while True:
        job_id = work_queue.get()
        try:
            process(job_id)
        finally:
            work_queue.task_done()


@app.on_event("startup")
def startup() -> None:
    init_db()
    threading.Thread(target=worker_loop, daemon=True, name="clipforge-worker").start()


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "clipforge-worker", "model_ready": MODEL_DIR.exists(),
            "queue_depth": work_queue.qsize(), "storage": str(DATA_DIR)}


@app.post("/v1/jobs", status_code=202)
def create_job(payload: JobRequest, x_clipforge_signature: str = Header(default="")) -> dict:
    signature_base = f"{payload.job_id}.{payload.project_id}.{payload.workspace_id}".encode()
    if SECRET and not hmac.compare_digest(
        hmac.new(SECRET.encode(), signature_base, hashlib.sha256).hexdigest(),
        x_clipforge_signature,
    ):
        raise HTTPException(403, "invalid signature")
    if payload.source.type == "upload" and not payload.source.url:
        raise HTTPException(400, "upload source requires a signed URL")
    with db() as conn:
        existing = conn.execute("SELECT id,status FROM jobs WHERE id=?", (payload.job_id,)).fetchone()
        if existing:
            return {"id": existing["id"], "status": existing["status"], "duplicate": True}
        timestamp = now()
        conn.execute("INSERT INTO jobs(id,project_id,request_json,status,progress,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                     (payload.job_id, payload.project_id, json.dumps(payload.model_dump(mode="json")), "queued", 0, timestamp, timestamp))
    work_queue.put(payload.job_id)
    return {"id": payload.job_id, "status": "accepted", "duplicate": False}


@app.get("/v1/jobs/{job_id}")
def get_job(job_id: str) -> dict:
    row = job_row(job_id)
    if not row:
        raise HTTPException(404, "job not found")
    return row


@app.get("/files/{job_id}/{filename}")
def get_file(job_id: str, filename: str):
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", filename):
        raise HTTPException(400, "invalid filename")
    path = DATA_DIR / job_id / filename
    if not path.is_file():
        raise HTTPException(404, "file not found")
    return FileResponse(path)
