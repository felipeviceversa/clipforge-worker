---
title: ClipForge Worker
emoji: 🎬
colorFrom: purple
colorTo: green
sdk: docker
app_port: 7860
pinned: false
license: apache-2.0
---

# ClipForge Worker

Worker CPU gratuito do ClipForge: FFmpeg + Vosk pt-BR + FastAPI.

## Variáveis obrigatórias

- `WORKER_WEBHOOK_SECRET`: mesmo segredo do Skip Cloud.
- `PUBLIC_BASE_URL`: URL pública do Space, sem barra final.

## Variáveis opcionais

- `DATA_DIR=/data/clipforge` (use `/tmp/clipforge` sem volume persistente)
- `MODEL_DIR=/models/vosk-model-small-pt-0.3`
- `MAX_DOWNLOAD_MB=1500`
- `RETENTION_HOURS=24`
- `CALLBACK_ENABLED=true`

## Endpoints

- `GET /health`
- `POST /v1/jobs`
- `GET /v1/jobs/{id}`
- `GET /files/{job_id}/{filename}`

A fila processa um vídeo por vez para caber no CPU Basic gratuito.
