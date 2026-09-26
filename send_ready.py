#!/usr/bin/env python3
"""Envia callback project.ready depois de publicar os assets no GitHub Release."""
import hashlib, hmac, json, os, time, uuid
from pathlib import Path
import httpx

result = json.loads(Path(os.environ.get('GITHUB_OUTPUT_JSON', 'job-result.json')).read_text())
req = result['request']
event_id = 'evt_' + uuid.uuid4().hex
occurred = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat().replace('+00:00','Z')
body = {'event_id': event_id, 'event_type': 'project.ready', 'job_id': req['job_id'],
        'project_id': req['project_id'], 'occurred_at': occurred, 'progress': 100, 'data': result['result']}
base = f"{event_id}.project.ready.{req['job_id']}.{occurred}".encode()
sig = hmac.new(os.environ['WORKER_WEBHOOK_SECRET'].encode(), base, hashlib.sha256).hexdigest()
for delay in [0, 10, 30, 120]:
    if delay: time.sleep(delay)
    try:
        r=httpx.post(req['callback_url'], json=body, headers={'X-ClipForge-Signature':sig}, timeout=20)
        if 200 <= r.status_code < 300:
            print(r.text); break
        if 400 <= r.status_code < 500 and r.status_code not in (408,429):
            raise SystemExit(f"callback rejected: {r.status_code} {r.text}")
    except httpx.TransportError:
        continue
else:
    raise SystemExit('callback failed after retries')
