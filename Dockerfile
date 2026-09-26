FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl unzip fonts-liberation && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user PATH=/home/user/.local/bin:$PATH PYTHONUNBUFFERED=1 PORT=7860
WORKDIR $HOME/app
COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=user . .
RUN mkdir -p /home/user/app/models /home/user/app/data && \
    curl -L --retry 3 -o /tmp/model.zip https://alphacephei.com/vosk/models/vosk-model-small-pt-0.3.zip && \
    unzip -q /tmp/model.zip -d /home/user/app/models && rm /tmp/model.zip
ENV MODEL_DIR=/home/user/app/models/vosk-model-small-pt-0.3 DATA_DIR=/home/user/app/data
EXPOSE 7860
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860}"]
