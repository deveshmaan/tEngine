# Setup & Secrets

- Export broker secrets through environment variables **before** starting the engine. Never commit them to `config/app.yml`.
- Required:
  - `UPSTOX_ACCESS_TOKEN`
- Optional:
  - `UPSTOX_API_KEY`
  - `UPSTOX_API_SECRET`
  - `ALLOWED_IPS` – comma-separated static IP allowlist (e.g., `203.0.113.10,203.0.113.11`). If set, the engine will refuse to start unless the current public IP is whitelisted.

Example:

```bash
export UPSTOX_ACCESS_TOKEN="paste-token-here"
export UPSTOX_API_KEY="your-api-key"
export UPSTOX_API_SECRET="your-api-secret"
export ALLOWED_IPS="203.0.113.10,203.0.113.11"
```

The canonical config (`config/app.yml`) should only contain non-sensitive knobs. Set `allowed_ips` there only if you are comfortable keeping the list in version control; environment variables always override YAML values.
