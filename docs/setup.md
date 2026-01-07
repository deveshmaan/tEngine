# Setup & Secrets

- Export broker secrets through environment variables **before** starting the engine. Never commit them to `config/app.yml`.
- Required:
  - `UPSTOX_CLIENT_ID`
  - `UPSTOX_CLIENT_SECRET`
  - `UPSTOX_REDIRECT_URI`
  - `UPSTOX_ACCESS_TOKEN`
- Optional:
  - `UPSTOX_ALGO_ID`
  - `UPSTOX_API_KEY` / `UPSTOX_API_SECRET` (legacy aliases for client ID/secret)
  - `ALLOWED_IPS` – comma-separated static IP allowlist (e.g., `203.0.113.10,203.0.113.11`). If set, the engine will refuse to start unless the current public IP is whitelisted.

Example:

```bash
export UPSTOX_CLIENT_ID="your-client-id"
export UPSTOX_CLIENT_SECRET="your-client-secret"
export UPSTOX_REDIRECT_URI="https://your-redirect-uri"
export UPSTOX_ACCESS_TOKEN="paste-token-here"
export UPSTOX_ALGO_ID="optional-algo-id"
export ALLOWED_IPS="203.0.113.10,203.0.113.11"
```

The canonical config (`config/app.yml`) should only contain non-sensitive knobs. Set `allowed_ips` there only if you are comfortable keeping the list in version control; environment variables always override YAML values.

## Secret scanning (pre-commit + CI)

Install and enable the pre-commit hook locally:

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

CI runs the same scan on every push/PR via GitHub Actions.
