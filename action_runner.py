#!/usr/bin/env python3
"""Executa um job ClipForge de forma síncrona dentro do GitHub Actions."""
import argparse, json, os, sqlite3, sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--job-id', required=True)
parser.add_argument('--project-id', required=True)
parser.add_argument('--workspace-id', required=True)
parser.add_argument('--source-type', required=True)
parser.add_argument('--source-url', required=True)
parser.add_argument('--language', default='pt-BR')
parser.add_argument('--prompt', default='')
parser.add_argument('--settings', default='{}')
parser.add_argument('--callback-url', required=True)
args = parser.parse_args()

sys.path.insert(0, str(Path(__file__).parent))
import app as worker

worker.init_db()
request = {
    'job_id': args.job_id, 'project_id': args.project_id, 'workspace_id': args.workspace_id,
    'source': {'type': args.source_type, 'url': args.source_url}, 'language': args.language,
    'prompt': args.prompt, 'settings': json.loads(args.settings or '{}'), 'callback_url': args.callback_url,
}
timestamp = worker.now()
with worker.db() as conn:
    conn.execute("INSERT OR REPLACE INTO jobs(id,project_id,request_json,status,progress,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                 (args.job_id, args.project_id, json.dumps(request), 'queued', 0, timestamp, timestamp))
worker.process(args.job_id)
row = worker.job_row(args.job_id)
Path(os.environ.get('GITHUB_OUTPUT_JSON', 'job-result.json')).write_text(json.dumps(row, ensure_ascii=False, indent=2))
if row['status'] != 'completed':
    raise SystemExit(row.get('error') or 'worker failed')
print(f"Completed {args.job_id}: {len(row['result'].get('previews', []))} previews")
